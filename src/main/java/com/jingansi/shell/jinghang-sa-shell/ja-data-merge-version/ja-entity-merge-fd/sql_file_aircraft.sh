#!/bin/bash

cur_time=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
-- 按天生成
-- 获取 只出现一次的 的飞机
-- 前面两天的 最大日期是 前面两天的
drop table if exists test.tmp_aircraft_only_one_01;

create table test.tmp_aircraft_only_one_01 as
select
	flight_id,
	count(*) as cnt
from sa.dwd_bhv_aircraft_combine_rt
where acquire_time between to_date(date_sub(now(),2)) and to_date(now()) -- 两天的日期
group by flight_id
having count(*)=1
  and max(acquire_time) < date_sub(now(),1); -- 截止到前一天凌晨只出现一次的


-- 计算出这两天飞机是否在只出现在一次的表中出现
create table test.tmp_01 as (
select flight_id from sa.dwd_bhv_aircraft_combine_rt
where acquire_time between  to_date(date_sub(now(),1)) and to_date(now())
group by flight_id
);

delete from sa.dim_aircraft_only_one_flight_id
using test.tmp_01
where sa.dim_aircraft_only_one_flight_id.flight_id = tmp_01.flight_id;

drop table if exists test.tmp_01;

-- 关联状态表 找出 其中 icao_code 注册号 航班号 机型 全为空的
insert into sa.dim_aircraft_only_one_flight_id
select
	flight_id,
	now() as update_time
from sa.dws_bhv_aircraft_last_location_rt
where flight_id in (
	select flight_id from test.tmp_aircraft_only_one_01
	)
and (registration is null or registration='')
and (callsign is null or callsign='')
and (flight_no is null or flight_no='')
and (flight_type is null or flight_type='');


insert into sa.dws_bhv_aircraft_last_location_fd
select
	flight_id
	,date_trunc('${cur_time}','day') as merge_time
	,acquire_time
	,src_code
	,icao_code
	,registration
	,flight_no
	,callsign
	,flight_type
	,longitude
	,latitude
	,speed_km
	,altitude_baro
	,altitude_baro_m
	,altitude_geom
	,altitude_geom_m
	,heading
	,squawk_code
	,flight_status
	,origin_airport3_code
	,origin_airport_e_name
	,origin_airport_c_name
	,origin_lng
	,origin_lat
	,dest_airport3_code
	,dest_airport_e_name
	,dest_airport_c_name
	,dest_lng
	,dest_lat
	,flight_departure_time
	,expected_landing_time
	,to_destination_distance
	,estimated_landing_duration
	,data_source
	,source
	,position_country_code2
	,friend_foe
	,filter_col
	,sea_id
	,sea_name
	,h3_code
	,update_time
from (

select
	*,
	row_number()over(partition by flight_id order by acquire_time desc) as rk
from (
  select
    *
  from sa.dws_bhv_aircraft_last_location_fd
  where merge_time = days_sub(date_trunc('${cur_time}','day'),1)

  union all

  select
	  *
  from sa.dws_bhv_aircraft_last_location_dh
  where merge_time = date_trunc('${cur_time}','day')
    and flight_id not in (select flight_id from sa.dim_aircraft_only_one_flight_id)
  ) as t1
) as t2
where rk = 1;

-- 关联删除数据
delete from sa.dws_bhv_aircraft_last_location_fd
using sa.dim_aircraft_only_one_flight_id
where sa.dws_bhv_aircraft_last_location_fd.flight_id = sa.dim_aircraft_only_one_flight_id.flight_id
and merge_time >= to_date(date_sub(now(),2));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
