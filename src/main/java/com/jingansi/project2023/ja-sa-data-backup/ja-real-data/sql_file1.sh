#!/bin/bash

# 假设2023-12-02 05:30开始跑脚本
# stary_day:2023-12-01
# end_day:2023-12-02
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

-- 船舶实体属性  update_time
insert into sa.dws_ais_vessel_detail_static_attribute
select
    *
from doris_ecs.sa.dws_ais_vessel_detail_static_attribute
where update_time between days_sub('${start_time}',1) and '${end_time} 00:00:00';


select sleep(5) as sleep1;


-- 船舶融合状态表  update_time
insert into sa.dws_ais_vessel_status_info
select
    *
from doris_ecs.sa.dws_ais_vessel_status_info
where update_time between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(5) as sleep2;


-- 船舶按天的数据  acquire_timestamp_format
insert into sa.dws_ais_vessel_all_info_day
select
    *
from doris_ecs.sa.dws_ais_vessel_all_info_day
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(5) as sleep3;


-- marinetraffic的船舶状态数据单独入库 update_time
insert into sa.dws_vessel_list_status_rt
select
    *
from doris_ecs.sa.dws_vessel_list_status_rt
where update_time between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep4;


-- 飞机实体融合状态表  update_time
insert into sa.dws_aircraft_combine_status_rt
select
    *
from doris_ecs.sa.dws_aircraft_combine_status_rt
where update_time
between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep5;


-- 飞机航班表 update_time
insert into sa.dws_flight_segment_rt
select
  *
from doris_ecs.sa.dws_flight_segment_rt
where update_time
between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep6;


-- marinetraffic的详情表 update_time
insert into sa.dwd_mtf_ship_info
select
  *
from doris_ecs.sa.dwd_mtf_ship_info
where update_time between days_sub('${start_time}',1) and '${end_time} 00:00:00';


select sleep(10) as sleep7;


-- vt的船舶状态数据单独入库 update_time
insert into sa.dws_vt_vessel_status_info
select
  *
from doris_ecs.sa.dws_vt_vessel_status_info
where update_time between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep8;


-- 卫星tle融合表,全量表1 current_date
insert into sa.dwd_satellite_tle_list
select
  *
from doris_ecs.sa.dwd_satellite_tle_list
where epoch_time between to_date(days_sub('${start_time}',2)) and '${end_time} 00:00:00';


select sleep(10) as sleep10;


-- 卫星tle融合表2，每日生成的表 current_date
insert into sa.dws_satellite_tle_info
select
  *
from doris_ecs.sa.dws_satellite_tle_info
where current_date between to_date(days_sub('${start_time}',2)) and '${end_time} 00:00:00';


"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
