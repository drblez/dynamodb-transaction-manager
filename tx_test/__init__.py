from dynamodb2.constructor import Field
from dynamodb2.transaction import Tx

__author__ = 'drblez'


def test():
    tx = Tx('Tx1', 'RC')
    #agent = tx.get_item('agent', '99')
    #accounts = tx.get_item('accounts-1', '99', 12345)
    #agent.update(Update('balance').add(42).dict())
    #accounts.update(Update('field42').put(4242).dict())
    tx.commit()
    print tx.stat
    tx = Tx('Tx1', 'RC')
    accounts = tx.get_item('accounts-1', '55', 12345)
    accounts.put(Field('f42', 42).dict())
    tx.commit()
    print tx.stat