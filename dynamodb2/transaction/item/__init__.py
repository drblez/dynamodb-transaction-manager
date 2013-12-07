# coding=utf-8

import simplejson as json

__author__ = 'drblez'

"""

    Таблица совместимости блокировок:

    ------------------+---------------+--------------
        Запрашиваемый !               !
        уровень  -->  ! Эксклюзивная  ! Разделяемая
    ------------------+---------------+--------------
        Текущий   |   !               !
        уровень   V   !               !
    ------------------+---------------+--------------
    Нет блокировки    ! Совместимо    ! Совместимо
    ------------------+---------------+--------------
    Эксклюзивная      ! Не совместимо ! Не совместимо
    ------------------+---------------+--------------
    Разделяемая       ! Не совместимо ! Совместимо
    ------------------+---------------+--------------


    Операции накладывают блокировки в аттрибуте tx_manager_data типа SS

    [
        '{ "tx_id": <tx_id>, "lock": "S"|"X" }',
        ...
    ]

"""

LOCK_EXCLUSIVE = 'X'
LOCK_SHARED = 'S'


class BadLockType(Exception):
    pass


class TxItem():
    def __init__(self, table_name, hash_key_value, range_key_value=None, tx=None):
        self.request = None
        self.tx = tx
        self.table_name = table_name
        self.hash_key_value = hash_key_value
        self.range_key_value = range_key_value
        self.key = self.tx.connection.gen_key_attribute(self.table_name, self.hash_key_value, self.range_key_value)

    def get_lock_data(self):
        attribute_to_get = ['tx_manager_data']
        consistent_read = True
        items = self.tx.connection.connection.get_item(self.table_name, self.key, attribute_to_get, consistent_read)
        items = items['Item']
        if items == {}:
            return []
        items = items['tx_manager_data']['SS']
        return map(json.loads, items)

    def lock_item(self, lock_type):
        if lock_type == LOCK_SHARED:
            data_value = {
                'tx_id': str(self.tx.tx_id),
                'lock': lock_type
            }
            attribute_updates = {
                'tx_manager_data': {
                    'Action': 'ADD',
                    'Value': {'SS': [json.dumps(data_value)]}
                }
            }
            self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates)
        elif lock_type == LOCK_EXCLUSIVE:
            expected = {
                'tx_manager_x_lock': {
                    'Exists': "false"
                }
            }
            data_value = str(self.tx.tx_id)
            attribute_updates = {
                'tx_manager_x_lock': {
                    'Action': 'PUT',
                    'Value': {'S': data_value}
                }
            }
            self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates, expected)
        else:
            raise BadLockType('Lock type is ' + lock_type)

    def get(self):
        pass

    def put(self, data):
        pass

    def update(self, data):
        pass

    def delete(self):
        pass
