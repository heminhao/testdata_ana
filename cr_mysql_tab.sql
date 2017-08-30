create table sel_userflow
(mdn text COLLATE utf8_unicode_ci,
starttime text COLLATE utf8_unicode_ci,
endtime text COLLATE utf8_unicode_ci,
destinationurl text COLLATE utf8_unicode_ci,
url_type text COLLATE utf8_unicode_ci,
host text COLLATE utf8_unicode_ci,
site text COLLATE utf8_unicode_ci,
download_bytes int,
upload_bytes int)
;

create table sel_bs
(prod_inst_num_first_enc varchar(500) COLLATE utf8_unicode_ci,
start_time varchar(100) COLLATE utf8_unicode_ci,
base_station varchar(100) COLLATE utf8_unicode_ci,
sector varchar(100) COLLATE utf8_unicode_ci,
power_flag varchar(100) COLLATE utf8_unicode_ci,
datelabel varchar(100) COLLATE utf8_unicode_ci,
loadstamp varchar(100) COLLATE utf8_unicode_ci)
;

ALTER TABLE `sel_bs` ADD INDEX inx_mdn_find(`prod_inst_num_first_enc`);

create table sel_bs_tag
(mdn varchar(200) COLLATE utf8_unicode_ci,
wday_out_time bigint, #工作日早上出门时间
wday_in_time bigint, #工作日晚上回家时间
eday_out_time bigint, #休息日早上出门时间
eday_in_time bigint, #休息日晚上回家时间
wd_move_loc bigint, #工作日活动地点特征汇总
we_move_loc bigint, #双休日活动地点特征汇总
PRIMARY KEY (`mdn`)
);

create table hive_bs_info
(bs_seq int,
sector varchar(200) COLLATE utf8_unicode_ci,
bsid varchar(100) COLLATE utf8_unicode_ci,
longitude varchar(100) COLLATE utf8_unicode_ci,
latitude varchar(100) COLLATE utf8_unicode_ci,
PRIMARY KEY (`bs_seq`))
;

create table all_bs_info
(bs_seq int,
bs_id varchar(100) COLLATE utf8_unicode_ci,
bs_sector varchar(200) COLLATE utf8_unicode_ci,
bs_longitude double,
bs_latitude double,
bs_addr varchar(1000) COLLATE utf8_unicode_ci,
bs_cat_seq int,
PRIMARY KEY (`bs_seq`))
;

create index inx_all_bs_id on all_bs_info(bs_id);

create table bs_cat_info
(bs_cat_seq int,
bs_cat_desc varchar(500) COLLATE utf8_unicode_ci,
bs_weight_mul bigint,
PRIMARY KEY (`bs_cat_seq`))
;

create table tmp_bs_name
(bs_seq int,
bs_addr varchar(1000) COLLATE utf8_unicode_ci,
PRIMARY KEY (`bs_seq`))
;

update all_bs_info t1
set t1.bs_addr = (select t2.bs_addr from tmp_bs_name t2
where t2.bs_seq=t1.bs_seq)
;


create table tmp_sel_bs
as
select * from sel_bs
where prod_inst_num_first_enc='8be6b5e167f0348a1cb4bf8dab46d07903ee9f42a929a6e2dc2e5375f91ab880'
;

create table tmp_sum
(mdn varchar(200) COLLATE utf8_unicode_ci,
cnt int)
;

mysqldump -uroot -p -e --opt --database softcomp > 20170807_softcomp.sql


DROP PROCEDURE IF EXISTS set_bs_tag;
DELIMITER $
CREATE PROCEDURE set_bs_tag()
  LANGUAGE SQL
  DETERMINISTIC
  MODIFIES SQL DATA
  SQL SECURITY DEFINER
BEGIN
  #DECLARE变量的声明必须放在开头，且在异常处理定义之前
  DECLARE v_mdn_no varchar(500);
  DECLARE v_finished_bit INT DEFAULT FALSE;
  DECLARE my_cursor CURSOR FOR ( SELECT distinct prod_inst_num_first_enc FROM tmp_sel_bs ORDER BY prod_inst_num_first_enc );
  DECLARE sig_bs_cur CURSOR FOR ( SELECT start_time,base_station,sector FROM tmp_sel_bs WHERE prod_inst_num_first_enc=v_mdn_no ORDER BY start_time );
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_finished_bit = TRUE;
  
  CREATE TEMPORARY TABLE tmp_run_sum
  (day_str varchar(20) COLLATE utf8_unicode_ci,
  bs_cat_seq int,
  day_type int, #日期类型，0表示工作日，1表示休息日
  bs_weight_mul bigint,
  time_sum bigint,
  PRIMARY KEY (`day_str`,`bs_cat_seq`));
  
  OPEN my_cursor;
  SET v_finished_bit=FALSE;
  myloop_tag:LOOP
    FETCH my_cursor INTO v_mdn_no;
    IF v_finished_bit THEN
      LEAVE myloop_tag;
    END IF;
    BEGIN
      DECLARE v_start_time varchar(100);
      DECLARE v_base_station varchar(100);
      DECLARE v_sector varchar(100);
      DECLARE v_inner_end_bit INT DEFAULT FALSE;
      DECLARE v_tot_out_time BIGINT;
      DECLARE v_tot_in_time BIGINT;
      DECLARE v_tot_plus int;
      DECLARE v_day_out_time BIGINT;
      DECLARE v_day_in_time BIGINT;
      DECLARE v_before_timestamp BIGINT;
      DECLARE v_curr_f_timestamp BIGINT;
      DECLARE v_curr_timestamp BIGINT;
      DECLARE v_before_bs varchar(100);
      DECLARE v_before_day varchar(100);
      DECLARE v_before_day_type int;
      DECLARE v_curr_day varchar(100);
      DECLARE v_bs_cat_seq int;
      DECLARE v_bs_seq int;
      DECLARE v_bs_weight_mul bigint;
      DECLARE v_bs_time_sum bigint;
      DECLARE v_curr_day_type int; #日期类型，0表示工作日，1表示休息日
      DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_inner_end_bit = TRUE;
      OPEN sig_bs_cur;
      SET v_inner_end_bit=FALSE;
      SET v_tot_out_time=0;
      SET v_tot_in_time=0;
      SET v_tot_plus=0;
      SET v_day_out_time=0;
      SET v_day_in_time=0;
      SET v_before_timestamp=0;
      SET v_before_bs='';
      SET v_before_day='';
      SET v_before_day_type=2;
      inloop_tag:LOOP
        FETCH sig_bs_cur INTO v_start_time,v_base_station,v_sector;
        IF v_inner_end_bit THEN
          LEAVE inloop_tag;
        END IF;
        BEGIN
          DECLARE v_no_data int;
          DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_data = TRUE;
          SET v_no_data = FALSE;
          SELECT bs_cat_seq INTO v_bs_cat_seq
            FROM all_bs_info
            WHERE (bs_id=v_base_station) and (right(bs_sector,1)=v_sector)
            LIMIT 1;
          if v_no_data then
            SELECT bs_cat_seq INTO v_bs_cat_seq
              FROM all_bs_info
              WHERE (bs_id=v_base_station)
              LIMIT 1;
          end if;
          #DECLARE EXIT HANDLER FOR NOT FOUND SELECT 'bs_weight_mul not found';
          SELECT bs_weight_mul INTO v_bs_weight_mul
            FROM bs_cat_info
            WHERE bs_cat_seq=v_bs_cat_seq;
        END;
        SET v_curr_day=substr(v_start_time,1,10);
        if v_curr_day='2016-07-26' then
          SET v_curr_day_type=0;
        elseif v_curr_day='2016-07-28' then
          SET v_curr_day_type=0;
        elseif v_curr_day='2016-07-30' then
          SET v_curr_day_type=1;
        elseif v_curr_day='2016-07-31' then
          SET v_curr_day_type=1;
        else
          SET v_curr_day_type=2;
        end if;
        if v_curr_day!=v_before_day then
          SET v_day_in_time=v_curr_f_timestamp;
          if v_before_day_type=0 then
            SET v_tot_out_time=v_tot_out_time+v_day_out_time;
            SET v_tot_in_time=v_tot_in_time+v_day_in_time;
            SET v_tot_plus=v_tot_plus+1;
          end if;
          SET v_day_out_time=0;
          SET v_day_in_time=0;
          SET v_before_timestamp=0;
          SET v_curr_f_timestamp=0;
          SET v_before_bs='';
          SET v_before_day=v_curr_day;
          SET v_before_day_type=v_curr_day_type;
        end if;
        SET v_curr_timestamp=(UNIX_TIMESTAMP(v_start_time)-UNIX_TIMESTAMP(v_curr_day));
        if v_before_timestamp!=0 then
          if v_base_station!=v_before_bs then
            if v_day_out_time=0 then
              SET v_day_out_time=v_curr_timestamp;
            end if;
            SET v_curr_f_timestamp=v_curr_timestamp;
          end if;
          SET v_bs_time_sum=v_curr_timestamp-v_before_timestamp;
        else
          SET v_bs_time_sum=0;
        end if;
        BEGIN
          DECLARE v_no_data int;
          DECLARE vv_day_str varchar(100);
          DECLARE vv_bs_cat_seq bigint;
          DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_data = TRUE;
          SET v_no_data = FALSE;
          SELECT day_str,bs_cat_seq INTO vv_day_str,vv_bs_cat_seq
            FROM tmp_run_sum
            WHERE (day_str=v_curr_day) and (bs_cat_seq=v_bs_cat_seq);
          if v_no_data then
            insert into tmp_run_sum(day_str,bs_cat_seq,day_type,bs_weight_mul,time_sum)
              values(v_curr_day,v_bs_cat_seq,v_curr_day_type,v_bs_weight_mul,v_bs_time_sum);
          else
            update tmp_run_sum
              set time_sum=time_sum+v_bs_time_sum
              where (day_str=vv_day_str) and (bs_cat_seq=vv_bs_cat_seq);
          end if;
        END;
        SET v_before_bs=v_base_station;
        SET v_before_timestamp=v_curr_timestamp;
      END LOOP inloop_tag;
      CLOSE sig_bs_cur;
      SET v_day_in_time=v_curr_f_timestamp;
      if v_curr_day_type=0 then
        SET v_tot_out_time=v_tot_out_time+v_day_out_time;
        SET v_tot_in_time=v_tot_in_time+v_day_in_time;
        SET v_tot_plus=v_tot_plus+1;
      end if;
      
      BEGIN
        DECLARE v_no_data int;
        DECLARE d_bs_cat_seq bigint;
        DECLARE d_time_sum bigint;
        DECLARE d_day_num bigint;
        DECLARE d_bs_weight_mul bigint;
        DECLARE d_move_loc bigint;
        DECLARE d_day_type int;
        DECLARE d_wd_move_loc bigint;
        DECLARE d_we_move_loc bigint;
        DECLARE tmp_sum_cur CURSOR FOR 
          ( SELECT bs_cat_seq,sum(time_sum),count(distinct day_str),min(bs_weight_mul)
            FROM tmp_run_sum
            WHERE day_type=d_day_type
            GROUP BY bs_cat_seq
            ORDER BY bs_cat_seq );
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_data = TRUE;
        
        SET d_day_type=0;
        while (d_day_type<=1) do
          SET v_no_data=FALSE;
          SET d_move_loc=0;
          OPEN tmp_sum_cur;
          tmpcur_tag:LOOP
            FETCH tmp_sum_cur INTO d_bs_cat_seq,d_time_sum,d_day_num,d_bs_weight_mul;
            IF v_no_data THEN
              LEAVE tmpcur_tag;
            END IF;
            SET d_move_loc=d_move_loc+round(d_time_sum/d_day_num/8640)*d_bs_weight_mul;
          END LOOP tmpcur_tag;
          CLOSE tmp_sum_cur;
          if d_day_type=0 then
            SET d_wd_move_loc=d_move_loc;
          else
            SET d_we_move_loc=d_move_loc;
          end if;
        end while;
      END;
      insert into sel_bs_tag(mdn,day_out_time,day_in_time,wd_move_loc,we_move_loc)
        values(v_mdn_no,round(v_tot_out_time/v_tot_plus),round(v_tot_in_time/v_tot_plus),d_wd_move_loc,d_we_move_loc);
      truncate table tmp_sum_cur;
    END;
  END LOOP myloop_tag;
  CLOSE my_cursor;
END;
$
DELIMITER ;



create table tmp_bs_man_cat
(bs_seq int,
bs_cat_desc varchar(500) COLLATE utf8_unicode_ci,
bs_cat_seq int,
PRIMARY KEY (`bs_seq`))
;

update tmp_bs_man_cat t1
set t1.bs_cat_seq=(select t2.bs_cat_seq from bs_cat_info t2
where t2.bs_cat_desc=t1.bs_cat_desc)
;

update tmp_bs_man_cat t1
set t1.bs_cat_desc='飞机火车地铁及枢纽站'
where t1.bs_cat_desc='火车地铁站'
;

update tmp_bs_man_cat t1
set t1.bs_cat_seq=(select t2.bs_cat_seq from bs_cat_info t2
where t2.bs_cat_desc=t1.bs_cat_desc)
where t1.bs_cat_seq is null
;

update all_bs_info t1
set t1.bs_cat_seq=(select t2.bs_cat_seq from tmp_bs_man_cat t2
where t2.bs_seq=t1.bs_seq)
where t1.bs_cat_seq is null
;

update all_bs_info t1
set t1.bs_cat_seq=5
where t1.bs_cat_seq is null
;

create table ori_test_data
(mdn varchar(200) COLLATE utf8_unicode_ci,
sex varchar(10) COLLATE utf8_unicode_ci,
age_period int COLLATE utf8_unicode_ci,
ad_mdn varchar(200) COLLATE utf8_unicode_ci)
;

create table ori_sig_data
(seq_id int,
mdn varchar(200) COLLATE utf8_unicode_ci,
sex varchar(10) COLLATE utf8_unicode_ci,
age_period int COLLATE utf8_unicode_ci,
PRIMARY KEY (`seq_id`)
)
;

create table ori_sex_cat
(sex_id int,
sex_desc varchar(10) COLLATE utf8_unicode_ci,
PRIMARY KEY (`sex_id`)
)
;

create table ori_cat_desc
(cat_id int,
sex varchar(10) COLLATE utf8_unicode_ci,
age_period int,
PRIMARY KEY (`cat_id`)
)
;


select init_seq('seq_tmp',1,1000000000,1);

insert into ori_sig_data(seq_id,mdn,sex,age_period)
select get_seq_nextval('seq_tmp'),mdn,min(sex),min(age_period)
from ori_test_data
group by mdn
;

select * from sel_bs_tag;


select t1.seq_id,t1.sex,t1.age_period,t3.cat_id,t2.*
from ori_sig_data t1,sel_bs_tag t2,ori_cat_desc t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex and t1.age_period=t3.age_period)
order by t1.seq_id;


select t3.cat_id,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2,ori_cat_desc t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex and t1.age_period=t3.age_period) and
(t1.seq_id>=2 and t1.seq_id<=1001)
order by t1.seq_id;


select wday_out_time,wday_in_time from sel_bs_tag
limit 2
INTO OUTFILE  '/var/lib/mysql/temp_demo.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;


select t3.cat_id,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2,ori_cat_desc t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex and t1.age_period=t3.age_period) and
(t1.seq_id>=2 and t1.seq_id<=1001)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_data.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn and
(seq_id>=2 and seq_id<=2001)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_age.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn and
(t1.seq_id>2001)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/test_age.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t3.sex_id,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2,ori_sex_cat t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex_desc) and
(seq_id>=2 and seq_id<=2001)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_sex.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t3.sex_id,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2,ori_sex_cat t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex_desc) and
(seq_id>2001)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/test_sex.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;


create table sel_bs_guess
(prod_inst_num_first_enc varchar(500) COLLATE utf8_unicode_ci,
start_time varchar(100) COLLATE utf8_unicode_ci,
base_station varchar(100) COLLATE utf8_unicode_ci,
sector varchar(100) COLLATE utf8_unicode_ci,
power_flag varchar(100) COLLATE utf8_unicode_ci,
datelabel varchar(100) COLLATE utf8_unicode_ci,
loadstamp varchar(100) COLLATE utf8_unicode_ci)
;

ALTER TABLE `sel_bs_guess` ADD INDEX inx_mdn_find(`prod_inst_num_first_enc`);

create table mdn_netdata
(mdn varchar(500) COLLATE utf8_unicode_ci,
starttime bigint,
endtime bigint,
download_bytes bigint,
upload_bytes bigint,
destinationurl varchar(2000) COLLATE utf8_unicode_ci,
useragent varchar(200) COLLATE utf8_unicode_ci,
host varchar(200) COLLATE utf8_unicode_ci,
site varchar(200) COLLATE utf8_unicode_ci,
datelabel varchar(100) COLLATE utf8_unicode_ci,
loadstamp varchar(100) COLLATE utf8_unicode_ci)
;

create table guess_netdata
(mdn varchar(500) COLLATE utf8_unicode_ci,
starttime bigint,
endtime bigint,
download_bytes bigint,
upload_bytes bigint,
destinationurl varchar(2000) COLLATE utf8_unicode_ci,
useragent varchar(200) COLLATE utf8_unicode_ci,
host varchar(200) COLLATE utf8_unicode_ci,
site varchar(200) COLLATE utf8_unicode_ci,
datelabel varchar(100) COLLATE utf8_unicode_ci,
loadstamp varchar(100) COLLATE utf8_unicode_ci)
;


select distinct t1.base_station from sel_bs_guess t1
where not exists (select 0 from all_bs_info t2
where t2.bs_id=t1.base_station)
order by t1.base_station;


create table add_bs_info
(bs_seq int,
bs_id varchar(100) COLLATE utf8_unicode_ci,
bs_sector varchar(200) COLLATE utf8_unicode_ci,
bs_longitude double,
bs_latitude double,
bs_addr varchar(1000) COLLATE utf8_unicode_ci,
bs_cat_seq int,
PRIMARY KEY (`bs_seq`))
;

create table tmp_add_bs_data
(bs_seq int,
bs_addr varchar(1000) COLLATE utf8_unicode_ci,
bs_cat_seq int,
PRIMARY KEY (`bs_seq`))
;

update add_bs_info t1
set t1.bs_addr =
(select t2.bs_addr from tmp_add_bs_data t2
where t2.bs_seq=t1.bs_seq)
;

update add_bs_info t1
set t1.bs_cat_seq =
(select t2.bs_cat_seq from tmp_add_bs_data t2
where t2.bs_seq=t1.bs_seq)
;

insert into all_bs_info
(bs_seq,
bs_id,
bs_sector,
bs_longitude,
bs_latitude,
bs_addr,
bs_cat_seq)
select
bs_seq,
bs_id,
bs_sector,
bs_longitude,
bs_latitude,
bs_addr,
bs_cat_seq
from add_bs_info
;

ALTER TABLE `sel_bs_guess` ADD INDEX inx_guess_mdn_find(`prod_inst_num_first_enc`);

create table sel_bs_guess_tag
(mdn varchar(200) COLLATE utf8_unicode_ci,
wday_out_time bigint, #工作日早上出门时间
wday_in_time bigint, #工作日晚上回家时间
eday_out_time bigint, #休息日早上出门时间
eday_in_time bigint, #休息日晚上回家时间
wd_move_loc bigint, #工作日活动地点特征汇总
we_move_loc bigint, #双休日活动地点特征汇总
PRIMARY KEY (`mdn`)
);

select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn and
(seq_id>=2 and seq_id<=1370)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_age_half.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn and
(t1.seq_id>1370)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/test_age_half.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select 0,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from sel_bs_guess_tag t2
order by t2.mdn
INTO OUTFILE  '/var/lib/mysql/test_age_guess.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn and
(t1.seq_id>1370) and (t1.age_period!=3)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/test_age_not3.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;


select t1.age_period,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2
where t1.mdn=t2.mdn
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_age_all.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

select t3.sex_id,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from ori_sig_data t1,sel_bs_tag t2,ori_sex_cat t3
where t1.mdn=t2.mdn and (t1.sex=t3.sex_desc)
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/train_sex_all.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;

create table tmp_guess_seq
(seq_id int,
age_id int,
PRIMARY KEY (`seq_id`)
);

alter table tmp_guess_seq add (sex_id int);

insert into tmp_guess_seq(seq_id)
select t1.seq_id
from guess_mdn_seq t1,sel_bs_guess_tag t2
where t1.mdn=t2.mdn
order by t1.seq_id
;

create table tmp_guess_res
(seq_id int,
age_id int,
PRIMARY KEY (`seq_id`)
);

create table tmp_guess_sex
(seq_id int,
sex_id int,
PRIMARY KEY (`seq_id`)
);


select 0,concat('1:',format(cast(t2.wday_out_time as double)/86400,18)),
concat('2:',format(cast(t2.wday_in_time as double)/86400,18)),
concat('3:',format(cast(t2.eday_out_time as double)/86400,18)),
concat('4:',format(cast(t2.eday_in_time as double)/86400,18)),
concat('5:',format(cast(t2.wd_move_loc as double)/1000000000000000,18)),
concat('6:',format(cast(t2.we_move_loc as double)/1000000000000000,18))
from guess_mdn_seq t1,sel_bs_guess_tag t2
where t1.mdn=t2.mdn
order by t1.seq_id
INTO OUTFILE  '/var/lib/mysql/test_age_guess.txt'
CHARACTER SET utf8
fields terminated by ' ' lines terminated by '\n'
;


create table guess_mdn_seq
(seq_id int,
mdn varchar(200) COLLATE utf8_unicode_ci,
PRIMARY KEY (`seq_id`)
);

alter table guess_mdn_seq add (age_id int,sex_id int);

update guess_mdn_seq t1
set t1.age_id = (select t2.age_id from tmp_guess_res t2
where t2.seq_id=t1.seq_id)
;

update guess_mdn_seq t1
set t1.sex_id = (select t2.sex_id from tmp_guess_sex t2
where t2.seq_id=t1.seq_id)
;

create table guess_mdn_seq_fill
as
select * from guess_mdn_seq
;

alter table guess_mdn_seq_fill add primary key(`seq_id`);

update guess_mdn_seq_fill
set age_id=1
where age_id is null;

update guess_mdn_seq_fill
set sex_id=1
where sex_id is null;
