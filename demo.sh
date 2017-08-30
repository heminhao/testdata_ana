#!/bin/bash

beg_str="20170101"
end_str="20170110"

curr_str=$beg_str
until [[ $curr_str -gt $end_str ]]
do
  echo $curr_str
  ss=`date -d "$curr_str 00:00:00" +%s`
  ss_new=`expr $ss + 86400`
  curr_str=`date -d @$ss_new +%Y%m%d`
done

