import uuid

__author__ = 'drblez'


class Tx():
    def __init__(self, tx_name, isolation_level):
        self.tx_id = uuid.uuid1()
        self.tx_name = tx_name
        self.isolation_level = isolation_level


class TxItem():
    def __init__(self, request):
        self.request = request


