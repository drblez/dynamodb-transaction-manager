from datetime import datetime
from time import sleep
import uuid
from boto.exception import JSONResponseError
from dynamodb2 import AWSDynamoDB2Connection
from dynamodb2.transaction.item import TxItem

__author__ = 'drblez'

TX_TABLE_NAME = 'tx-trans-man-tx-info'
TX_DATA_TABLE_NAME = 'tx-trans-man-tx-data'

ISOLATION_LEVEL_FULL_LOCK = '000 full lock'
ISOLATION_LEVEL_READ_COMMITTED = '100 read committed'
ISOLATION_LEVEL_READ_UNCOMMITTED = '200 read uncommitted'


class BadTxTableAttributes(Exception):
    pass


class BadTxTableKeySchema(Exception):
    pass


def check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name):
    try:
        t = connection.describe_table(table_name)
        t = t['Table']
        if t['AttributeDefinitions'] != attribute_definition:
            raise BadTxTableAttributes('Table {} has attributes {}, need {}'.format(
                table_name,
                t['AttributeDefinitions'],
                attribute_definition))
        if t['KeySchema'] != key_schema:
            raise BadTxTableKeySchema('Table {} has key schema {}, need {}'.format(
                table_name,
                t['KeySchema'],
                key_schema))
    except JSONResponseError as e:
        if e.error_code == 'ResourceNotFoundException':
            connection.create_table(attribute_definition, table_name, key_schema, provisioned_throughput)
            while True:
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
    provisioned_throughput = {
        'ReadCapacityUnits': read_capacity_units,
        'WriteCapacityUnits': write_capacity_units
    }
    check_or_create_table(attribute_definition, connection, key_schema, provisioned_throughput, table_name)


class Tx():
    def __init__(self, tx_name, isolation_level, tx_table_name=TX_TABLE_NAME, tx_data_table_name=TX_DATA_TABLE_NAME,
                 aws_credential=None):
        self.tx_id = uuid.uuid1()
        self.tx_name = tx_name
        self.isolation_level = isolation_level
        self.creation_date = datetime.now().isoformat()
        if aws_credential is None:
            self.connection = AWSDynamoDB2Connection().connection
        else:
            self.connection = AWSDynamoDB2Connection(
                aws_credential.access_key,
                aws_credential.secret_key,
                aws_credential.region).connection
        check_or_create_tx_table(self.connection, tx_table_name)
        check_or_create_tx_data_table(self.connection, tx_data_table_name)

    def get_item(self, table_name, hash_key_value, range_key_value):
        tx_item = TxItem(table_name, hash_key_value, range_key_value)
        tx_item.tx = self

    def commit(self):
        pass

    def rollback(self):
        pass