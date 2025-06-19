#!/bin/bash

start_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_atr_aircraft_image_id_info
select
	flight_photo as image_id,
	flight_id,
	max(acquire_time) as acquire_time,
	'正常' as remark,
	null as create_by,
	now() as update_time
from sa.dwd_bhv_aircraft_combine_rt
where acquire_time between days_sub(date_trunc('${start_time}','day'),1) and date_trunc('${start_time}','day')
	and src_code = 1
	and flight_photo is not null
	and flight_photo <> ''
group by flight_photo,flight_id;


insert into sa.dws_atr_aircraft_image_info
select
  t1.*
from (
  select
  	flight_id,
  	src_code,
  	acquire_time,
  	image_id,
  	image_url,
  	remark,
  	create_by,
  	update_time
  from (
  select
  	t2.flight_id,
  	1 as src_code,
  	t2.acquire_time,
  	t1.flight_photo as image_id,
  	t1.minio_url as image_url,
  	null as remark,
  	null as create_by,
  	t1.update_time,
  	row_number()over(partition by t2.flight_id order by length(t1.minio_url) desc ) as rk
  from (
  select * from sa.dws_aircraft_image_info
  where update_time between days_sub(date_trunc('${start_time}','day'),3) and date_trunc('${start_time}','day')
   ) as t1
  inner join (
  	select * from sa.dws_atr_aircraft_image_id_info
  	where update_time between days_sub(date_trunc('${start_time}','day'),3) and date_trunc('${start_time}','day')
   ) as t2
  on t1.flight_photo = t2.image_id
  ) as t1
  where rk = 1
) as t1
left join sa.dws_atr_aircraft_image_info as t2
on t1.flight_id = t2.flight_id
where t2.remark is null

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
