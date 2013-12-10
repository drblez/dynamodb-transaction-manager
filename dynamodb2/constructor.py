from datetime import datetime
import decimal

__author__ = 'drblez'

"""

    Field('f1', 'value1').field('f2', 'value2').field('f3', 42).field('f4', ['a', 'b', 'c']).field('f5', [1, 2, 3]).dict

    {
        'f1': {'S': 'value1'),
        'f2': {'S': 'value2'},
        'f3': {'N': '42'},
        'f4': {'SS': ['a', 'b', 'c']},
        'f5': {'NS': [1, 2, 3]}
    }

    Update('f3').add(1).also(Update('f4').add(['d'])).also(Update('f5').delete([2, 3])).also(Update('f6').put(0)).
    also(Update('f1').delete()).dict()

    {
        'f3': {'Value': {'N': '1'}, 'Action': 'ADD'}
        'f4': {'Value': {'SS': ['d']}, 'Action': 'ADD'}
        'f5': {'Value': {'NS': ['2', '3'], Action: 'DELETE'}
        'f6': {'Action': 'DELETE'}
    }

    Expected('f1', True, 'value1').expected('f3', True, 42).expected('f6', False).dict()

    {
        'f1': {'Value': {'S', 'value1'}, 'Exists': true}
        'f2': {'Value': {'N', '42'}, 'Exists': true}
        'f6': {'Exists': false}
    }

    KeyConditions('f3').between(40, 44).also(KeyConditions('f1').eq('value1')).dict()

    {
        'f3': {'AttributeValueList': [{'N': '40'}, {'N': '44'}], 'ComparisonOperator': 'BETWEEN'},
        'f1': {'AttributeValueList': [{'S', 'value1'}], 'ComparisonOperator': 'EQ'}
    }

"""


class EmptyList(Exception):
    pass


class BadDynamoDBType(Exception):
    pass


class ActionAlreadyExists(Exception):
    pass


class ExpectedError(Exception):
    pass


def dynamodb_type(value):
    if type(value) == str:
        return 'S'
    elif type(value) == int:
        return 'N'
    elif type(value) == float:
        return 'N'
    elif type(value) == decimal.Decimal:
        return 'N'
    elif type(value) == datetime:
        return 'D'
    elif type(value) == list:
        if len(value) == 0:
            raise EmptyList()
        return dynamodb_type(value[0]) + 'S'
    else:
        raise BadDynamoDBType('Bad type {} of value {}'.format(type(value), value))


class Field():
    def __init__(self, name, value):
        self.name = name
        self.type = dynamodb_type(value)
        if self.type in ['SS', 'NS']:
            t = []
            for v in value:
                t.append(str(v))
            self.value = t
        elif self.type == 'D':
            self.type = 'S'
            self.value = value.isoformat()
        elif self.type == 'DS':
            self.type = 'SS'
            t = []
            for v in value:
                t.append(v.isoformat())
            self.value = t
        else:
            self.value = str(value)
        self.items = [self]

    def field(self, name, value):
        f = Field(name, value)
        self.items.append(f)
        return self

    def dict(self):
        d = {}
        for i in self.items:
            d[i.name] = {i.type: i.value}
        return d


class Update():
    def __init__(self, field):
        self.field = field
        self.action = None
        self.value = None
        self.items = []

    def add(self, value):
        if not self.action is None:
            raise ActionAlreadyExists('For field {} exists action {}'.format(self.field, self.action))
        self.value = Field('Value', value).dict()
        self.action = 'ADD'
        self.items.append(self)
        return self

    def put(self, value):
        self.value = Field('Value', value).dict()
        self.action = 'PUT'
        self.items.append(self)
        return self

    def delete(self, value=None):
        if not value is None:
            self.value = Field('Value', value).dict()
        self.action = 'DELETE'
        self.items.append(self)
        return self

    def also(self, update):
        self.items.append(update)
        return self

    def dict(self):
        d = {}
        for i in self.items:
            if not i.value is None:
                t = i.value
            else:
                t = {}
            t['Action'] = i.action
            d[i.field] = t
        return d


class Expected():
    def __init__(self, field, exists, value=None):
        self.field = field
        self.exists = str(exists).lower()
        if exists and (value is None):
            raise ExpectedError('Exists true and Value is None not compatible')
        if value is None:
            self.value = None
        else:
            self.value = Field('Value', value).dict()
        self.items = [self]

    def expected(self, field, exists, value=None):
        e = Expected(field, exists, value)
        self.items.append(e)
        return self

    def dict(self):
        d = {}
        for i in self.items:
            if not i.value is None:
                t = i.value
            else:
                t = {}
            t['Exists'] = i.exists
            d[i.field] = t
        return d


class KeyConditions():
    def __init__(self, field):
        self.field = field
        self.items = []
        self.operator = None
        self.values = []

    def between(self, lower, upper):
        v1 = Field('Value', lower).dict()['Value']
        v2 = Field('Value', upper).dict()['Value']
        self.values = [v1, v2]
        self.operator = 'BETWEEN'
        self.items.append(self)
        return self

    def __operator(self, operator, value):
        self.values = [value]
        self.operator = operator
        self.items.append(self)
        return self

    def eq(self, value):
        return self.__operator('EQ', value)

    def le(self, value):
        return self.__operator('LE', value)

    def lt(self, value):
        return self.__operator('LT', value)

    def ge(self, value):
        return self.__operator('GE', value)

    def gt(self, value):
        return self.__operator('GT', value)

    def begins_with(self, value):
        return self.__operator('BEGINS_WITH', value)

    def also(self, key_conditions):
        self.items.append(key_conditions)
        return self

    def dict(self):
        d = {}
        for i in self.items:
            d[i.field] = {
                'AttributeValueList': i.values,
                'ComparisonOperator': i.operator
            }
        return d
