# a tool for sync data from hive to sql

## 为什么不使用pyhs2或者通过thrift RPC调用hiveservice?
实践生产环节中使用了阿里云EMR，在使用thrift或者pyhs2包时，会报错Tsocket read 0 bytes。经过搜索后"问题出现在protocol的协议使用上，python要使用TCompactProtocol，而不能使用TBinaryProtocol。" 原文链接未保存，此处就不给出了。

## 为什么不使用sqoop？
因为真的很不好用。。。

## 脚本实现的功能为执行shell命令生成文件到本地，读取本地文件写入到sql，使用了peewee

## 具体使用
* 生成数据模型 $:python -m pwiz -e mysql -H localhost -p3306 -uxzone -Ppasswd sql_db_name > db.py
* 编辑配置文件 tableinfo.py 示例如下：
```
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
```

* hive select中的每列字段名需要和sql中的列名一致。
* 命令行使用方式
```
[hadoop@emr-header-1 sync]$ ./sync.py -h
usage: sync.py [-h] [-t TABLELIST] [-d NOWDATE] [-a]

optional arguments:
  -h, --help            show this help message and exit
  -t TABLELIST, --tables TABLELIST
                        table name list that will be sync. eg: a,b,c
  -d NOWDATE, --day NOWDATE
                        date string like 2016-08-20
  -a, --all             sync all tables or not
[hadoop@emr-header-1 sync]$ ./sync.py -d 2016-08-20 -a
```
