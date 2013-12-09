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


def __check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name,
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


def _check_or_create_tx_table(connection, table_name=TX_TABLE_NAME,
                              read_capacity_units=5, write_capacity_units=5):
    attribute_definition = [
        {
            'AttributeName': 'tx_uuid',
            'AttributeType': 'S'
        }
    ]
    key_schema = [
        {
            'AttributeName': 'tx_uuid',
            'KeyType': 'HASH'
        }
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': read_capacity_units,
        'WriteCapacityUnits': write_capacity_units
    }
    __check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name)


def _check_or_create_tx_data_table(connection, table_name=TX_DATA_TABLE_NAME,
                                   read_capacity_units=5, write_capacity_units=5):
    attribute_definition = [
        {
            'AttributeName': 'tx_uuid',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'log_uuid',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'rec_uuid',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'creation_date',
            'AttributeType': 'S'
        }
    ]
    key_schema = [
        {
            'AttributeName': 'tx_uuid',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'log_uuid',
            'KeyType': 'RANGE'
        }
    ]
    local_secondary_indexes = [
        {
            'IndexName': 'creation_date-index',
            'KeySchema': [
                {
                    'AttributeName': 'tx_uuid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'creation_date',
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {'ProjectionType': 'ALL'}
        },
        {
            'IndexName': 'rec_uuid-index',
            'KeySchema': [
                {
                    'AttributeName': 'tx_uuid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'rec_uuid',
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
    __check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name,
                            local_secondary_indexes=local_secondary_indexes)


class Tx():
    def __init__(self, tx_name, isolation_level, tx_table_name=TX_TABLE_NAME, tx_data_table_name=TX_DATA_TABLE_NAME,
                 aws_credential=None):
        self.tx_uuid = uuid.uuid1()
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
        _check_or_create_tx_table(self.connection.connection, self.tx_table_name)
        _check_or_create_tx_data_table(self.connection.connection, self.tx_data_table_name)
        self.tx_items = []
        self.key = {'tx_uuid': {'S': str(self.tx_uuid)}}
        self.tx_log = []
        expected = {
            'tx_uuid': {'Exists': 'false'}
        }
        tx_record = {
            'tx_uuid': {'S': str(self.tx_uuid)},
            'tx_name': {'S': self.tx_name},
            'isolation_level': {'S': self.isolation_level},
            'creation_date': {'S': self.creation_date},
            'status': {'S': 'START'}
        }
        self.connection.connection.put_item(self.tx_table_name, tx_record, expected=expected)

    def __add_rec_uuid_to_tx(self, tx_item):
        expected = {
            'tx_uuid': {
                'Exists': 'true',
                'Value': {'S': str(self.tx_uuid)}
            }
        }
        update_rec = {
            'recs': {
                'Action': 'ADD',
                'Value': {'SS': [str(tx_item.rec_uuid)]}
            },
            'status': {
                'Action': 'PUT',
                'Value': {'S': 'IN-FLIGHT'}
            }
        }
        self.connection.connection.update_item(self.tx_table_name, self.key, update_rec, expected=expected)

    def __add_log_uuid_to_tx(self, log_uuid):
        expected = {
            'tx_uuid': {
                'Exists': 'true',
                'Value': {'S': str(self.tx_uuid)}
            }
        }
        update_rec = {
            'logs': {
                'Action': 'ADD',
                'Value': {'SS': [str(log_uuid)]}
            },
            'status': {
                'Action': 'PUT',
                'Value': {'S': 'IN-FLIGHT'}
            }
        }
        self.connection.connection.update_item(self.tx_table_name, self.key, update_rec, expected=expected)

    def get_item(self, table_name, hash_key_value, range_key_value=None):
        """

        Put item information into inner transaction structures and return item descriptor

        @rtype : TxItem
        @param table_name: DynamoDB table name
        @param hash_key_value: Hash value
        @param range_key_value: Range value (if present)
        @return: TxItem instance
        """
        tx_item = TxItem(table_name, hash_key_value, range_key_value, self)
        self.__add_rec_uuid_to_tx(tx_item)
        self.tx_items.append(tx_item)
        return tx_item

    def _put_tx_log(self, tx_item, data, operation):
        log_uuid = uuid.uuid1()
        log_record = {
            'tx_uuid': {'S': str(self.tx_uuid)},
            'log_uuid': {'S': str(log_uuid)},
            'rec_uuid': {'S': str(tx_item.rec_uuid)},
            'creation_date': {'S': datetime.now().isoformat()},
            'table': {'S': tx_item.table_name},
            'key': {'S': json.dumps(tx_item.key)},
            'operation': {'S': operation}
        }
        if not data is None:
            log_record['data'] = {'S': json.dumps(data)}
        expected = {
            'tx_uuid': {'Exists': 'false'},
            'log_uuid': {'Exists': 'false'}
        }
        logger.debug('Log record: {}'.format(log_record))
        logger.debug('Expected: {}'.format(expected))
        self.tx_log.append(log_record)
        result = self.connection.connection.put_item(
            self.tx_data_table_name,
            log_record,
            expected=expected,
            return_values='ALL_OLD')
        self.__add_log_uuid_to_tx(log_uuid)
        return result

    def __set_tx_status(self, status):
        expected = {
            'tx_uuid': {
                'Exists': 'true',
                'Value': {'S': str(self.tx_uuid)}
            }
        }
        update_rec = {
            'status': {
                'Action': 'PUT',
                'Value': {'S': status}
            }
        }
        self.connection.connection.update_item(self.tx_table_name, self.key, update_rec, expected=expected)

    def _unlock_all_items(self):
        for tx_item in self.tx_items:
            tx_item.unlock()

    def commit(self):
        self._unlock_all_items()
        self.__set_tx_status('COMMIT')

    def rollback(self):
        while len(self.tx_log) > 0:
            log_record = self.tx_log.pop()
            table_name = log_record['table']['S']
            operation = log_record['operation']['S']
            if operation == 'PUT':
                data = json.loads(log_record['data']['S'])['Attributes']
                logger.debug('PUT Table: {}, data: {}'.format(table_name, data))
                self.connection.connection.put_item(table_name, data)
            elif operation == 'DELETE':
                key = json.loads(log_record['key']['S'])
                logger.debug('PUT Table: {}, key: {}'.format(table_name, key))
                self.connection.connection.delete_item(table_name, key)
        self.__set_tx_status('ROLLBACK')
        self._unlock_all_items()
