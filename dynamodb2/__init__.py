import boto.dynamodb2

__author__ = 'drblez'


class AWSDynamoDB2Connection():
    def __init__(self, access_key, secret_access_key, region):
        self.connection = boto.dynamodb2.connect_to_region(
            region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key)