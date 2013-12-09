__author__ = 'drblez'


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