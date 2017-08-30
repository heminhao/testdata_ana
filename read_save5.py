#!/usr/bin/python_curr
# coding=utf-8

import sqlalchemy as sy
import csv
import os
import io

db_engine_url = 'mysql+pymysql://root:root123@10.4.227.9/softcomp?charset=utf8'
read_dir = 'd:\\temp1\\'

db_engine = sy.create_engine(db_engine_url)
db_conn = db_engine.connect()
db_meta = sy.MetaData()
db_tab_sel_userflow = sy.Table('tmp_bs_man_cat', db_meta, autoload=True, autoload_with=db_engine)
ins_stat = db_tab_sel_userflow.insert()
print('db conn ok')

eachfile = 'bs_group_left_filter.csv'
print(read_dir + eachfile + ' file begin dumping...')
f = io.open(read_dir + eachfile, 'r', encoding='utf8')
field_names = ['bs_seq', 'nouse1', 'nouse2', 'nouse3', 'bs_addr', 'bs_cat_desc']
readerdata = csv.DictReader(f, fieldnames=field_names, dialect='excel')
try:
    for rec in readerdata:
        db_conn.execute(ins_stat, rec)
except Exception as e:
    raise e
f.close()
print(read_dir + eachfile + ' file dumped...')
db_conn.close()
print('finished dumping data.')
