# coding=utf-8
__author__ = 'drblez'

"""

    Таблица совместимости блокировок:

    ------------------+---------------+--------------
        Запрашиваемый !               !
        уровень       ! Эксклюзивная  ! Разделяемая
    ------------------+---------------+--------------
        Текущий       !               !
        уровень       !               !
    ------------------+---------------+--------------
    Нет блокировки    ! Совместимо    ! Совместимо
    ------------------+---------------+--------------
    Эксклюзивная      ! Не совместимо ! Не совместимо
    ------------------+---------------+--------------
    Разделяемая       ! Не совместимо ! Совместимо
    ------------------+---------------+--------------


    Операции накладывают блокировки в аттрибуте tx_data типа SS

    [
        '{ "tx_id": <tx_id>, "lock": "S"|"X" }',
        ...
    ]

"""


class TxItem():
    def __init__(self, table_name, hash_key_value, range_key_value=None):
        self.request = None
        self.tx = None
        self.table_name = table_name
        self.hash_key_value = hash_key_value
        self.range_key_value = range_key_value

    def _lock_item(self):
        pass


    def get(self):
        pass

    def put(self, data):
        pass

    def update(self, data):
        pass

    def delete(self):
        pass
