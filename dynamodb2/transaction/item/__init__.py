__author__ = 'drblez'


class TxItem():
    def __init__(self, table_name, hash_key_value, range_key_value=None):
        self.request = None
        self.tx = None
        self.table_name = table_name
        self.hash_key_value = hash_key_value
        self.range_key_value = range_key_value

    def get(self):
        pass

    def put(self, data):
        pass

    def update(self, data):
        pass

    def delete(self):
        pass