#!/usr/bin/env python
# coding=utf-8
from db import *
import sys
import commands
import argparse
import time

from tableinfo import tables

"""
HOW TO:
1.generate orm data model
eg: python -m pwiz -e mysql -H localhost -p3306 -uroot -P'passwd' sqldb_name > db.py
2.edit tableinfo.py
"""

def camel_to_underline(camel_format):
    '''
    驼峰命名格式转下划线命名格式
    '''
    underline_format=''
    if isinstance(camel_format, str):
        for _s_ in camel_format:
            underline_format += _s_ if _s_.islower() else '_'+_s_.lower()
    return underline_format

def underline_to_camel(underline_format):
    '''
    下划线命名格式驼峰命名格式
    '''
    camel_format = ''
    if isinstance(underline_format, str):
        for _s_ in underline_format.split('_'):
            camel_format += _s_.capitalize()
    return camel_format

def Init_table_obj(classname, colnames, row):
    tableclass = eval(classname)
    tableobj = tableclass()
    for i,col in enumerate(colnames):
        if hasattr(tableobj, col):
            setattr(tableobj, col, row[i])
        elif col[-3:-1] == "_id" and hasattr(tableobj,col[0:-3]): #peewee always truncate some_id to some
            setattr(tableobj, col[0:-3], row[i])
        else:
            print "ERROR colname %s cannot find in mysql table " % col
            exit(-1)
    return tableobj

def push_to_mysql(NowDate,tlist):
    global tables
    for i,tab in enumerate(tables):
        if tab['table'] not in tlist:
            continue
        class_name = underline_to_camel(tab['table'])
        resultfile = open(tab['table']+'.result')
        columnline = resultfile.readline()
        columnline = columnline.strip().split('\t')

        colnames = []
        for col in columnline:
            col = col.strip().split('.')
            col = col[len(col)-1]
            colnames.append(col)

        # delete old data of mysql
        if tab['needarg']:
            (eval(class_name)).raw(tab['del'] % NowDate).execute()
        else:
            (eval(class_name)).raw(tab['del']).execute()

        data_source,i = [], 0
        for line in resultfile:
            line = line.strip().split('\t')
            objdict = Init_table_obj(class_name, colnames, line)
            data_source.append(objdict)
            i += 1
            if i == 1000:
                try:
                    # Fastest.
                    with database.atomic():
                        eval(class_name).insert_many(data_source).execute()
                    i = 0
                    data_source = []
                except Exception as e:
                    print "ERROR: save %s to mysql exception %s" %(line, e)
                    exit(-1)
        if len(data_source) > 0:
            try:
                with database.atomic():
                    eval(class_name).insert_many(data_source).execute()
            except Exception as e:
                print "ERROR: save %s to mysql exception %s" %(line, e)
                exit(-1)

        print "push tab %s over" % tab['table']

def pull_from_hive(NowDate,tlist):
    global tables
    for i,tab in enumerate(tables):
        if tab['table'] not in tlist:
            continue
        if tab['needarg']:
            hivecmd = tab['sel'] % NowDate
        else:
            hivecmd = tab['sel']

        hivecmd = "hive -e \"set hive.cli.print.header=true; " + hivecmd + "\""
        hivecmd += " >%s.result" % tab['table']
        print "execute hive cmd %s" % hivecmd
        (status, output) = commands.getstatusoutput(hivecmd)
        if status != 0 :
            print "PANIC job "+ tab['table'] +" error occur!"
            print output
            exit(-1)
        print output
        print "finish job: " + tab['table'] + "\n"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tables", dest="tablelist", default=[], help="table name list that will be sync. eg: a,b,c ")
    parser.add_argument("-d", "--day", dest="nowdate", default=time.strftime("%Y-%m-%d", time.localtime())  ,help="date string like 2016-08-20")
    parser.add_argument("-a", "--all", dest="allsync", default=False, action='store_true', help="sync all tables or not")

    args = parser.parse_args()

    if args.allsync:
        tablelist = [t['table'] for t in tables]
    else:
        tablelist = args.tablelist
        tablelist = tablelist.strip().split(',')
    NowDate = args.nowdate
    print "sync Data(%s) from hive to mysql tables %s begin..." % (NowDate, tablelist)
    pull_from_hive(NowDate,tablelist)
    push_to_mysql(NowDate,tablelist)
