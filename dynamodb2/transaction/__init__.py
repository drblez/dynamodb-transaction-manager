import uuid
from dynamodb2 import AWSDynamoDB2Connection
from dynamodb2.aws_credential import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

__author__ = 'drblez'

TX_TABLE_NAME = 'tx-trans-man-tx-info'
TX_DATA_TABLE_NAME = 'tx-trans-man-tx-data'


def check_or_create_tx_table(dynamodb2_connection, table_name=TX_TABLE_NAME):
    pass


def check_or_create_tx_data_table(dynamodb2_connection, table_name=TX_DATA_TABLE_NAME):
    pass


class Tx():
    def __init__(self, tx_name, isolation_level, tx_table_name=TX_TABLE_NAME, tx_data_table_name=TX_DATA_TABLE_NAME,
                 aws_credential=None):
        self.tx_id = uuid.uuid1()
        self.tx_name = tx_name
        self.isolation_level = isolation_level
        self.connection = AWSDynamoDB2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 'eu-west-1')
        self.tx_table = check_or_create_tx_table(self.connection, tx_table_name)
        self.tx_table_name = check_or_create_tx_data_table(self.connection, tx_data_table_name)


