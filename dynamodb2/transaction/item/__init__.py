# coding=utf-8
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
import simplejson as json

__author__ = 'drblez'

"""

    Lock levels compatibility table:

    ------------------+----------------+---------------
        Requested     !                !
        level    -->  ! Exclusive      ! Shared
    ------------------+----------------+---------------
        Current   |   !                !
        level     V   !                !
    ------------------+----------------+---------------
    No lock           ! Compatible     ! Compatible
    ------------------+----------------+---------------
    Exclusive         ! Non compatible ! Non compatible
    ------------------+----------------+---------------
    Shared            ! Non compatible ! Compatible
    ------------------+----------------+---------------


    Операции накладывают блокировки в аттрибуте tx_manager_data типа SS

    [
        '{ "tx_id": <tx_id>, "lock": "S"|"X" }',
        ...
    ]

"""

LOCK_EXCLUSIVE = 'X'
LOCK_SHARED = 'S'

LOCKS_DATA_FIELD = 'tx_manager_locks'
X_LOCK_DATA_FIELD = 'tx_manager_x_lock'


class BadLockType(Exception):
    pass


class TxItem():
    def __init__(self, table_name, hash_key_value, range_key_value=None, tx=None):
        self.request = None
        self.tx = tx
        self.tx_id_str = str(tx.tx_id)
        self.table_name = table_name
        self.hash_key_value = hash_key_value
        self.range_key_value = range_key_value
        self.key = self.tx.connection.gen_key_attribute(self.table_name, self.hash_key_value, self.range_key_value)
        self.lock_state = None

    def get_locks(self):
        attribute_to_get = [LOCKS_DATA_FIELD]
        consistent_read = True
        items = self.tx.connection.connection.get_item(self.table_name, self.key, attribute_to_get, consistent_read)
        items = items['Item']
        if items == {}:
            return []
        items = items[LOCKS_DATA_FIELD]['SS']
        locks = []
        print self.tx_id_str
        for item in items:
            item = json.loads(item)
            print item
            if item['tx_id'] != self.tx_id_str:
                locks.append(item)
        return locks

    def _x_lock(self):
        expected = {
            X_LOCK_DATA_FIELD: {
                'Exists': 'false'
            }
        }
        data_value = self.tx_id_str
        attribute_updates = {
            X_LOCK_DATA_FIELD: {
                'Action': 'PUT',
                'Value': {'S': data_value}
            }
        }
        self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates, expected)

    def _lock(self, lock_state, after_x_lock=False):
        if after_x_lock:
            expected = None
        else:
            expected = {
                X_LOCK_DATA_FIELD: {
                    'Exists': 'false'
                }
            }
        data_value = {
            'tx_id': self.tx_id_str,
            'lock': lock_state
        }
        attribute_updates = {
            LOCKS_DATA_FIELD: {
                'Action': 'ADD',
                'Value': {'SS': [json.dumps(data_value)]}
            }
        }
        self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates, expected)

    def _unlock(self):
        try:
            data_value = self.tx_id_str
            expected = {
                X_LOCK_DATA_FIELD: {
                    'Value': {'S': data_value},
                    'Exists': 'true'
                }
            }
            attribute_updates = {
                X_LOCK_DATA_FIELD: {
                    'Action': 'DELETE'
                }
            }
            self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates, expected)
        except ConditionalCheckFailedException:
            pass
        data_value_1 = {
            'tx_id': self.tx_id_str,
            'lock': LOCK_EXCLUSIVE
        }
        data_value_2 = {
            'tx_id': self.tx_id_str,
            'lock': LOCK_SHARED
        }
        attribute_updates = {
            LOCKS_DATA_FIELD: {
                'Action': 'DELETE',
                'Value': {'SS': [json.dumps(data_value_1), json.dumps(data_value_2)]}
            }
        }
        self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates)
        self.lock_state = None

    def lock(self, requested_lock_state):
        print 'Current lock state is {}, requested lock state is {}'.format(self.lock_state, requested_lock_state)
        if self.lock_state != requested_lock_state:
            if requested_lock_state == LOCK_SHARED:
                print 'S lock state'
                if self.lock_state == LOCK_EXCLUSIVE:
                    print 'Return True because self has X lock state'
                    return True
                locks = self.get_locks()
                for lock in locks:
                    if lock['lock'] == LOCK_EXCLUSIVE:
                        print 'Item already X locked'
                        return False
                print 'Set S lock on item'
                self._lock(requested_lock_state)
                self.lock_state = requested_lock_state
                return True
            elif requested_lock_state == LOCK_EXCLUSIVE:
                print 'X lock state'
                locks = self.get_locks()
                if len(locks) == 0:
                    print 'No any locks found'
                    self._x_lock()
                    self._lock(requested_lock_state, after_x_lock=True)
                    data_value = {
                        'tx_id': self.tx_id_str,
                        'lock': LOCK_SHARED
                    }
                    attribute_updates = {
                        LOCKS_DATA_FIELD: {
                            'Action': 'DELETE',
                            'Value': {'SS': [json.dumps(data_value)]}
                        }
                    }
                    self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates)
                    self.lock_state = requested_lock_state
                    return True
                else:
                    print 'Any locks found'
                    return False
            else:
                raise BadLockType('Lock type is ' + requested_lock_state)
        else:
            return True

    def unlock(self):
        self._unlock()

    def _get(self):
        pass

    def get(self):
        self.lock(LOCK_SHARED)
        return self._get()

    def put(self, data):
        pass

    def update(self, data):
        pass

    def delete(self):
        pass
