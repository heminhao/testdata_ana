use softcomp;
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
      DECLARE v_etot_out_time BIGINT;
      DECLARE v_etot_in_time BIGINT;
      DECLARE v_etot_plus int;
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
      DECLARE d_wd_move_loc bigint;
      DECLARE d_we_move_loc bigint;
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
      SET v_curr_f_timestamp=0;
      SET v_etot_out_time=0;
      SET v_etot_in_time=0;
      SET v_etot_plus=0;
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
        if v_bs_cat_seq is null then
          SET v_bs_cat_seq=15;
          SET v_bs_weight_mul=0;
        end if;
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
            if (v_day_out_time!=0) or (v_day_in_time!=0) then
              SET v_tot_plus=v_tot_plus+1;
            end if;
          elseif v_before_day_type=1 then
            SET v_etot_out_time=v_etot_out_time+v_day_out_time;
            SET v_etot_in_time=v_etot_in_time+v_day_in_time;
            if (v_day_out_time!=0) or (v_day_in_time!=0) then
              SET v_etot_plus=v_etot_plus+1;
            end if;
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
      elseif v_curr_day_type=1 then
        SET v_etot_out_time=v_etot_out_time+v_day_out_time;
        SET v_etot_in_time=v_etot_in_time+v_day_in_time;
        SET v_etot_plus=v_etot_plus+1;
      end if;
      
      BEGIN
        DECLARE v_no_data int;
        DECLARE d_bs_cat_seq bigint;
        DECLARE d_time_sum bigint;
        DECLARE d_day_num bigint;
        DECLARE d_bs_weight_mul bigint;
        DECLARE d_move_loc bigint;
        DECLARE d_tmp_loc bigint;
        DECLARE d_day_type int;
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
            SET d_tmp_loc=round(d_time_sum/d_day_num/8640);
            if d_tmp_loc<=0 then
              SET d_tmp_loc=1;
            end if;
            if d_tmp_loc>=10 then
              SET d_tmp_loc=9;
            end if;
            SET d_move_loc=d_move_loc+d_tmp_loc*d_bs_weight_mul;
          END LOOP tmpcur_tag;
          CLOSE tmp_sum_cur;
          if d_day_type=0 then
            SET d_wd_move_loc=d_move_loc;
          else
            SET d_we_move_loc=d_move_loc;
          end if;
          SET d_day_type=d_day_type+1;
        end while;
      END;
      if v_tot_plus=0 then
        SET v_tot_plus=1;
      end if;
      if v_etot_plus=0 then
        SET v_etot_plus=1;
      end if;
      insert into sel_bs_tag(mdn,wday_out_time,wday_in_time,eday_out_time,eday_in_time,wd_move_loc,we_move_loc)
        values(v_mdn_no,round(v_tot_out_time/v_tot_plus),round(v_tot_in_time/v_tot_plus),
        round(v_etot_out_time/v_etot_plus),round(v_etot_in_time/v_etot_plus),d_wd_move_loc,d_we_move_loc);
      truncate table tmp_run_sum;
    END;
  END LOOP myloop_tag;
  CLOSE my_cursor;
END;
$
DELIMITER ;

