from datetime import datetime
import logging
from time import sleep
import uuid
import sys

import simplejson as json
from boto.exception import JSONResponseError

from dynamodb2 import AWSDynamoDB2Connection
from dynamodb2.transaction.item import TxItem


__author__ = 'drblez'

TX_TABLE_NAME = 'tx-trans-man-tx-info'
TX_DATA_TABLE_NAME = 'tx-trans-man-tx-data'

ISOLATION_LEVEL_FULL_LOCK = '000 full lock'
ISOLATION_LEVEL_READ_COMMITTED = '100 read committed'
ISOLATION_LEVEL_READ_UNCOMMITTED = '200 read uncommitted'

logger = logging.getLogger('item')
logger.addHandler(logging.StreamHandler(stream=sys.stderr))
logger.setLevel(logging.DEBUG)


class BadTxTableAttributes(Exception):
    pass


class BadTxTableKeySchema(Exception):
    pass


def check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name,
                          local_secondary_indexes=None):
    try:
        t = connection.describe_table(table_name)
        t = t['Table']
        if sorted(t['AttributeDefinitions']) != sorted(attribute_definition):
            raise BadTxTableAttributes('Table {} has attributes {}, need {}'.format(
                table_name,
                t['AttributeDefinitions'],
                attribute_definition))
        if sorted(t['KeySchema']) != sorted(key_schema):
            raise BadTxTableKeySchema('Table {} has key schema {}, need {}'.format(
                table_name,
                t['KeySchema'],
                key_schema))
    except JSONResponseError as e:
        if e.error_code == 'ResourceNotFoundException':
            connection.create_table(attribute_definition, table_name, key_schema, provisioned_throughput,
                                    local_secondary_indexes=local_secondary_indexes)
            while True:
                logger.debug('Wait for table {} became ACTIVE'.format(table_name))
                sleep(10)
                t = connection.describe_table(table_name)
                if t['Table']['TableStatus'] == 'ACTIVE':
                    break
        else:
            raise e


def check_or_create_tx_table(connection, table_name=TX_TABLE_NAME,
                             read_capacity_units=5, write_capacity_units=5):
    attribute_definition = [
        {
            'AttributeName': 'tx_id',
            'AttributeType': 'S'
        }
    ]
    key_schema = [
        {
            'AttributeName': 'tx_id',
            'KeyType': 'HASH'
        }
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': read_capacity_units,
        'WriteCapacityUnits': write_capacity_units
    }
    check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name)


def check_or_create_tx_data_table(connection, table_name=TX_DATA_TABLE_NAME,
                                  read_capacity_units=5, write_capacity_units=5):
    attribute_definition = [
        {
            'AttributeName': 'tx_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'rec_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'creation_date',
            'AttributeType': 'S'
        }
    ]
    key_schema = [
        {
            'AttributeName': 'tx_id',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'rec_id',
            'KeyType': 'RANGE'
        }
    ]
    local_secondary_indexes = [
        {
            'IndexName': 'tx_id-index',
            'KeySchema': [
                {
                    'AttributeName': 'tx_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'creation_date',
                    'KeyType': 'RANGE'
                }

            ],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': read_capacity_units,
        'WriteCapacityUnits': write_capacity_units
    }
    check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name,
                          local_secondary_indexes=local_secondary_indexes)


class Tx():
    def __init__(self, tx_name, isolation_level, tx_table_name=TX_TABLE_NAME, tx_data_table_name=TX_DATA_TABLE_NAME,
                 aws_credential=None):
        self.tx_id = uuid.uuid1()
        self.tx_name = tx_name
        self.isolation_level = isolation_level
        self.creation_date = datetime.now().isoformat()
        if aws_credential is None:
            self.connection = AWSDynamoDB2Connection()
        else:
            self.connection = AWSDynamoDB2Connection(
                aws_credential.access_key,
                aws_credential.secret_key,
                aws_credential.region)
        self.tx_table_name = tx_table_name
        self.tx_data_table_name = tx_data_table_name
        check_or_create_tx_table(self.connection.connection, self.tx_table_name)
        check_or_create_tx_data_table(self.connection.connection, self.tx_data_table_name)
        self.tx_items = []
        self.key = {'tx_id': {'S': str(self.tx_id)}}
        self.tx_log = []

    def get_item(self, table_name, hash_key_value, range_key_value=None):
        tx_item = TxItem(table_name, hash_key_value, range_key_value, self)
        self.tx_items.append(tx_item)
        return tx_item

    def put_tx_log(self, key, data, operation):
        rec_id = {
            'creation_date': datetime.now().isoformat(),
            'id': str(uuid.uuid1())
        }
        log_record = {
            'tx_id': {'S': str(self.tx_id)},
            'rec_id': {'S': json.dumps(rec_id)},
            'creation_date': {'S': rec_id['creation_date']},
            'key': {'S': json.dumps(key)},
            'operation': {'S': operation}
        }
        if not data is None:
            log_record['data'] = json.dumps(data)
        expected = {
            'tx_id': {'Exists': 'false'},
            'rec_id': {'Exists': 'false'}
        }
        result = self.connection.connection.put_item(
            self.tx_data_table_name,
            log_record,
            expected=expected,
            return_values='ALL_OLD')
        return result

    def commit(self):
        for tx_item in self.tx_items:
            tx_item.unlock()

    def rollback(self):
        pass