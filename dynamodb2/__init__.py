import boto.dynamodb2
from dynamodb2.aws_credential import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

__author__ = 'drblez'


class AWSDynamoDB2Connection():
    def __init__(self,
                 access_key=AWS_ACCESS_KEY_ID,
                 secret_access_key=AWS_SECRET_ACCESS_KEY,
                 region=AWS_REGION):
        self.connection = boto.dynamodb2.connect_to_region(
            region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key)