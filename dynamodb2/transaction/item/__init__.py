# coding=utf-8
import logging
from time import sleep
import uuid
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
        '{ "tx_uuid": <tx_uuid>, "lock": "S"|"X" }',
        ...
    ]

"""

LOCK_EXCLUSIVE = 'X'
LOCK_SHARED = 'S'

LOCKS_DATA_FIELD = 'tx_manager_locks'
X_LOCK_DATA_FIELD = 'tx_manager_x_lock'

logger = logging.getLogger('item')
logger.setLevel(logging.DEBUG)


class BadLockType(Exception):
    pass


class LockWaitTime(Exception):
    pass


class NotExistingItem(Exception):
    pass


class TxItem():
    def __init__(self, table_name, hash_key_value, range_key_value=None, tx=None):
        self.request = None
        self.tx = tx
        self.tx_uuid_str = str(tx.tx_uuid)
        self.table_name = table_name
        self.hash_key_value = hash_key_value
        self.range_key_value = range_key_value
        self.key = self.tx.connection.gen_key_attribute(self.table_name, self.hash_key_value, self.range_key_value)
        self.lock_state = None
        self.not_exist = None
        self.rec_uuid = uuid.uuid1()

    def get_locks(self):
        attribute_to_get = [LOCKS_DATA_FIELD]
        consistent_read = True
        items = self.tx.connection.connection.get_item(self.table_name, self.key, attribute_to_get, consistent_read)
        if items == {}:
            raise NotExistingItem('Item with key {} not exist'.format(str(self.key)))
        items = items['Item']
        if items == {}:
            return []
        items = items[LOCKS_DATA_FIELD]['SS']
        locks = []
        logger.debug('Tx ID: {}'.format(self.tx_uuid_str))
        for item in items:
            item = json.loads(item)
            logger.debug('Item: {}'.format(item))
            if item['tx_uuid'] != self.tx_uuid_str:
                locks.append(item)
        return locks

    def _x_lock(self):
        expected = {
            X_LOCK_DATA_FIELD: {
                'Exists': 'false'
            }
        }
        data_value = self.tx_uuid_str
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
            'tx_uuid': self.tx_uuid_str,
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
            data_value = self.tx_uuid_str
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
            'tx_uuid': self.tx_uuid_str,
            'lock': LOCK_EXCLUSIVE
        }
        data_value_2 = {
            'tx_uuid': self.tx_uuid_str,
            'lock': LOCK_SHARED
        }
        attribute_updates = {
            LOCKS_DATA_FIELD: {
                'Action': 'DELETE',
                'Value': {'SS': [json.dumps(data_value_1), json.dumps(data_value_2)]}
            }
        }
        self.tx.connection.connection.update_item(self.table_name, self.key, attribute_updates)

    def lock(self, requested_lock_state):
        logger.debug('Current lock state is {}, requested lock state is {}'.format(self.lock_state,
                                                                                   requested_lock_state))
        if self.lock_state != requested_lock_state:
            if requested_lock_state == LOCK_SHARED:
                logger.debug('S lock state')
                if self.lock_state == LOCK_EXCLUSIVE:
                    logger.debug('Return True because self has X lock state')
                    return True
                locks = self.get_locks()
                for lock in locks:
                    if lock['lock'] == LOCK_EXCLUSIVE:
                        logger.debug('Item already X locked')
                        return False
                logger.debug('Set S lock on item')
                self._lock(requested_lock_state)
                self.lock_state = requested_lock_state
                return True
            elif requested_lock_state == LOCK_EXCLUSIVE:
                logger.debug('X lock state')
                locks = self.get_locks()
                if len(locks) == 0:
                    logger.debug('No any locks found')
                    self._x_lock()
                    self._lock(requested_lock_state, after_x_lock=True)
                    data_value = {
                        'tx_uuid': self.tx_uuid_str,
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
                    logger.debug('Any locks found')
                    return False
            else:
                raise BadLockType('Lock type is ' + requested_lock_state)
        else:
            return True

    def wait_lock(self, requested_lock_state, wait_time=0.1, max_wait_time=1, generate_exception=True):
        count = 0.0
        while not self.lock(requested_lock_state):
            count += wait_time
            if count > max_wait_time:
                if generate_exception:
                    raise LockWaitTime('Exceed {} sec.'.format(max_wait_time))
                return False
            sleep(wait_time)
        return True

    def unlock(self):
        self._unlock()
        self.lock_state = None

    def _get(self, attributes_to_get=None, consistent_read=True, return_consumed_capacity=None):
        result = self.tx.connection.connection.get_item(
            self.table_name, self.key,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            return_consumed_capacity=return_consumed_capacity)
        return result

    def get(self, attributes_to_get=None, consistent_read=True, return_consumed_capacity=None):
        self.wait_lock(LOCK_SHARED)
        return self._get(attributes_to_get, consistent_read, return_consumed_capacity)

    def _put(self, item, expected=None, return_values=None, return_consumed_capacity=None,
             return_item_collection_metrics=None):
        for k in self.key.keys():
            item[k] = self.key[k]
        result = self.tx.connection.connection.put_item(
            self.table_name, item, expected=expected, return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        return result

    def _add_x_lock_to_item(self, item):
        data_value = {
            'tx_uuid': self.tx_uuid_str,
            'lock': LOCK_EXCLUSIVE
        }
        item[X_LOCK_DATA_FIELD] = {'S': self.tx_uuid_str}
        item[LOCKS_DATA_FIELD] = {'SS': [json.dumps(data_value)]}

    def put(self, item, expected=None, return_consumed_capacity=None,
            return_item_collection_metrics=None):
        return_values = 'ALL_OLD'
        try:
            self.wait_lock(LOCK_EXCLUSIVE)
            if expected is None:
                expected = {}
            expected[X_LOCK_DATA_FIELD] = {
                'Value': {'S': self.tx_uuid_str},
                'Exists': 'true'
            }
            self._add_x_lock_to_item(item)
            result = self._put(item, expected=expected, return_values=return_values,
                               return_consumed_capacity=return_consumed_capacity,
                               return_item_collection_metrics=return_item_collection_metrics)
            self.tx._put_tx_log(self, result, 'PUT')
            return result
        except NotExistingItem:
            expected = self.key.copy()
            for k in self.key.keys():
                expected[k] = {'Exists': 'false'}
                item[k] = self.key[k]
            self._add_x_lock_to_item(item)
            logger.debug('Expected value: {}'.format(str(expected)))
            logger.debug('Item value: {}'.format(str(item)))
            result = self._put(item, expected=expected, return_values=return_values,
                               return_consumed_capacity=return_consumed_capacity,
                               return_item_collection_metrics=return_item_collection_metrics)
            self.tx._put_tx_log(self, None, 'DELETE')
            return result

    def _update(self, attribute_updates=None, expected=None, return_values=None, return_consumed_capacity=None,
                return_item_collection_metrics=None):
        for k in self.key.keys():
            try:
                del attribute_updates[k]
            except KeyError:
                pass
        logger.debug('Attribute updates: {}'.format(attribute_updates))
        logger.debug('Expected: {}'.format(expected))
        result = self.tx.connection.connection.update_item(
            self.table_name, self.key, attribute_updates=attribute_updates, expected=expected,
            return_values=return_values, return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)
        return result

    def update(self, update_data, expected=None, return_consumed_capacity=None,
               return_item_collection_metrics=None):
        return_values = 'ALL_OLD'
        try:
            self.wait_lock(LOCK_EXCLUSIVE)
            if expected is None:
                expected = {}
            expected[X_LOCK_DATA_FIELD] = {
                'Value': {'S': self.tx_uuid_str},
                'Exists': 'true'
            }
            result = self._update(
                attribute_updates=update_data, expected=expected, return_values=return_values,
                return_consumed_capacity=return_consumed_capacity,
                return_item_collection_metrics=return_item_collection_metrics)
            self.tx._put_tx_log(self, result, 'PUT')
            return result
        except NotExistingItem:
            raise NotExistingItem('Cannot update non existent item with key {}'.format(self.key))

    def delete(self):
        pass