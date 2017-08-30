#!/usr/bin/python_curr
# coding=utf-8

import sqlalchemy as sy
import csv
import os
import io

db_engine_url = 'mysql+pymysql://root:root123@10.4.227.9/softcomp?charset=utf8'
read_dir = 'd:\\temp1\\sel_bs\\'

db_engine = sy.create_engine(db_engine_url)
db_conn = db_engine.connect()
db_meta = sy.MetaData()
db_tab_sel_userflow = sy.Table('sel_bs', db_meta, autoload=True, autoload_with=db_engine)
ins_stat = db_tab_sel_userflow.insert()
print('db conn ok')

all_files = os.listdir(path = read_dir)
for eachfile in all_files :
    print(read_dir + eachfile + ' file begin dumping...')
    f = io.open(read_dir + eachfile, 'r', encoding='utf8')
    field_names = ['prod_inst_num_first_enc', 'start_time', 'base_station', 'sector', 'power_flag', 'datelabel', 'loadstamp']
    readerdata = csv.DictReader(f, fieldnames=field_names, dialect='excel-tab')
    try:
        for rec in readerdata:
            db_conn.execute(ins_stat, rec)
    except Exception as e:
        raise e
    f.close()
    print(read_dir + eachfile + ' file dumped...')
db_conn.close()
print('finished dumping data.')
