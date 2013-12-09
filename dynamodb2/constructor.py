__author__ = 'drblez'

"""

    Field('f1', 'value1').field('f2', 'value2').field('f3', 42).field('f4', ['a', 'b', 'c']).field('f5', [1, 2, 3]).dict

    {
        'f1': {'S': 'value1'),
        'f2': {'S': 'value2'},
        'f3': {'N': '42'},
        'f4': {'SS': ['a', 'b', 'c']},
        'f5': {'NN': [1, 2, 3]}
    }

    Update('f3').add(1).and(Update('f4').add(['d'])).and(Update('f5').delete([2, 3])).and(Update('f6').put(0))

"""


class EmptyList(Exception):
    pass


class BadDynamoDBType(Exception):
    pass


def dynamodb_type(value):
    if type(value) == str:
        return 'S'
    elif type(value) == int:
        return 'N'
    elif type(value) == float:
        return 'N'
    elif type(value) == list:
        if len(value) == 0:
            raise EmptyList()
        return dynamodb_type(value[0]) * 2
    else:
        raise BadDynamoDBType('Bad type {} of value {}'.format(type(value), value))


class Field():
    def __init__(self, name, value):
        self.name = name
        self.type = dynamodb_type(value)
        self.value = value

    def dict(self):
        return {self.name: {self.type: str(self.value)}}


class Update():
    def __init__(self, field, action, new_value):
        self.field = field
        self.action = action
        self.new_value = new_value

    def dict(self):
        pass