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

```
    from dynamodb2.constructor import *

    Field('f1', 'value1').field('f2', 'value2').field('f3', 42).field('f4', ['a', 'b', 'c']).
    field('f5', [1, 2, 3]).dict ==
    {
        'f1': {'S': 'value1'),
        'f2': {'S': 'value2'},
        'f3': {'N': '42'},
        'f4': {'SS': ['a', 'b', 'c']},
        'f5': {'NS': [1, 2, 3]}
    }

    Update('f3').add(1).also(Update('f4').add(['d'])).also(Update('f5').delete([2, 3])).also(Update('f6').put(0)).
    also(Update('f1').delete()).dict() ==
    {
        'f3': {'Value': {'N': '1'}, 'Action': 'ADD'}
        'f4': {'Value': {'SS': ['d']}, 'Action': 'ADD'}
        'f5': {'Value': {'NS': ['2', '3'], Action: 'DELETE'}
        'f6': {'Action': 'DELETE'}
    }

    Expected('f1', True, 'value1').expected('f3', True, 42).expected('f6', False).dict() ==
    {
        'f1': {'Value': {'S', 'value1'}, 'Exists': true}
        'f2': {'Value': {'N', '42'}, 'Exists': true}
        'f6': {'Exists': false}
    }

    KeyConditions('f3').between(40, 44).also(KeyConditions('f1').eq('value1')).dict() ==
    {
        'f3': {'AttributeValueList': [{'N': '40'}, {'N': '44'}], 'ComparisonOperator': 'BETWEEN'},
        'f1': {'AttributeValueList': [{'S', 'value1'}], 'ComparisonOperator': 'EQ'}
    }
```