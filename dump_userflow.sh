#!/bin/bash

while read line
do
  tmp_data=`echo $line | cut -d / -f 1`
  dl_val=${tmp_data#*=}
  tmp_data=`echo $line | cut -d / -f 2`
  ls_val=${tmp_data#*=}
  echo "dl_val = $dl_val ; ls_val = $ls_val"
  hive -d d_dl=$dl_val -d d_ls=$ls_val -f userflow_sql.sql
done < part_sel.txt

