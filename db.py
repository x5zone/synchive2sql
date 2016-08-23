from peewee import *
# example, not real
database = MySQLDatabase('sqldb_name', **{'host': 'localhost', 'password': 'passwd', 'port': 3306, 'user': 'root'})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class AcctGroup(BaseModel):
    id = BigIntegerField(primary_key=True)
    name = CharField(unique=True)

    class Meta:
        db_table = 'acct_group'
