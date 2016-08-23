# coding=utf-8

"""
every table has 4 attr：
table： table name of mysql db
sel： select sql that pull data from hive
del: delete sql that execute before push data to mysql db
needarg: sel&del sql need date arg or not
"""
tables = [
    {"table":  "aaa",
     "sel": "select name, aaage as age from hive_db.aaa where dt = '%s';",
     "del": "delete from aaa where dt = '%s'",
     "needarg": True,
    },
    {"table":  "bbb",
     "sel": "select name, bbbge as age from hive_db.bbb",
     "del": "delete from bbb",
     "needarg": False,
    },
]
