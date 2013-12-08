dynamodb-transaction-manager
============================

``` python
    ...
    tx = Tx('Add into shopping cart', ISOLATION_LEVEL_READ_COMMITTED)
    user = tx.get_item('user', user_name)
    cart = tx.get_item('cart', user_name)
    user.update({'items_counter': {'Action': 'ADD', 'Value': {'N': 1}}})
    cart.put({'item': {'S': item_name}})
    tx.commit()
    ...
```