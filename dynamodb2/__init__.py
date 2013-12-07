import boto.dynamodb2
from dynamodb2.aws_credential import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

__author__ = 'drblez'


class KeyAttributeError(Exception):
    pass


def convert_key_value(hash_key_value, hash_key_type):
    if hash_key_type == 'S':
        return str(hash_key_value)
    elif hash_key_type == 'N':
        return str(hash_key_value)
    else:
        return hash_key_value


class AWSDynamoDB2Connection():
    def __init__(self,
                 access_key=AWS_ACCESS_KEY_ID,
                 secret_access_key=AWS_SECRET_ACCESS_KEY,
                 region=AWS_REGION):
        self.connection = boto.dynamodb2.connect_to_region(
            region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key)

    def get_table_descriptor(self, table_name):
        table_descriptor = self.connection.describe_table(table_name)
        table_descriptor = table_descriptor['Table']
        return table_descriptor

    def get_key_name(self, table_name):
        t = self.get_table_descriptor(table_name)
        key_schema = t['KeySchema']
        keys = [key_schema[0]['AttributeName']]
        if len(key_schema) > 1:
            keys.append(key_schema[1]['AttributeName'])
        return keys

    def gen_key_attribute(self, table_name, hash_key_value, range_key_value=None):
        t = self.get_table_descriptor(table_name)
        key_schema = t['KeySchema']
        attributes = t['AttributeDefinitions']
        hash_key_name = None
        range_key_name = None
        hash_key_type = None
        range_key_type = None
        for key in key_schema:
            if key['KeyType'] == 'HASH':
                hash_key_name = key['AttributeName']
            elif key['KeyType'] == 'RANGE':
                range_key_name = key['AttributeName']
        for attr in attributes:
            if attr['AttributeName'] == hash_key_name:
                hash_key_type = attr['AttributeType']
            elif attr['AttributeName'] == range_key_name:
                range_key_type = attr['AttributeType']
        key = {
            hash_key_name: {
                hash_key_type: convert_key_value(hash_key_value, hash_key_type)
            }
        }
        if not range_key_name is None:
            if range_key_value is None:
                raise KeyAttributeError('Range key is not specified')
            key[range_key_name] = {
                range_key_type: convert_key_value(range_key_value, range_key_type)
            }
        return key