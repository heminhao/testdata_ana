use u_zwzx_hmh;

set d_dl;
set d_ls;

insert OVERWRITE TABLE sel_userflow
select
mdn,
starttime,
endtime,
destinationurl,
url_type,
host,
site,
sum(download_bytes),
sum(upload_bytes)
from test_data t1 join cdpi.sada_cdpi_userflow t2 on 
(t1.dev_no=t2.mdn) and (datelabel='${hivevar:d_dl}') and (loadstamp='${hivevar:d_ls}')
group by mdn,starttime,endtime,destinationurl,url_type,host,site
;
