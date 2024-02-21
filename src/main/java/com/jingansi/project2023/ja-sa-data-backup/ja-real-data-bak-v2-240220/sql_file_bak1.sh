#!/bin/bash

# 假设2023-12-02 05:30开始跑脚本
# stary_day:2023-12-01
# end_day:2023-12-02
start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

-- 表：dws_ais_vessel_detail_static_attribute -- 全量静态属性表  update_time
insert into sa.dws_ais_vessel_detail_static_attribute
select
    vessel_id
     ,imo
     ,mmsi
     ,callsign
     ,year_built
     ,vessel_type
     ,vessel_type_name
     ,vessel_class
     ,vessel_class_name
     ,name
     ,c_name
     ,draught_average
     ,speed_average
     ,speed_max
     ,length
     ,width
     ,height
     ,owner
     ,risk_rating
     ,risk_rating_name
     ,service_status
     ,service_status_name
     ,flag_country_code
     ,country_name
     ,source
     ,gross_tonnage
     ,deadweight
     ,rate_of_turn
     ,null as is_on_my_fleet
     ,null as is_on_shared_fleet
     ,null as is_on_own_fleet
     ,timestamp
     ,update_time
from doris_ecs.sa.dws_ais_vessel_detail_static_attribute
where update_time between to_date(days_sub('${start_time}',2)) and to_date(days_add('${start_time}',1));


select sleep(5) as sleep1;


-- 表：dws_ais_vessel_status_info -- 船舶状态表  update_time
insert into sa.dws_ais_vessel_status_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nation_flag_minio_url_jpg
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dws_ais_vessel_status_info
where update_time between '${start_time} 00:00:00' and '${start_time} 23:59:59';


select sleep(5) as sleep2;


-- 表：dws_ais_vessel_all_info_day -- 船舶按天  acquire_timestamp_format
insert into sa.dws_ais_vessel_all_info_day
select
    *
from doris_ecs.sa.dws_ais_vessel_all_info_day
where acquire_timestamp_format between '${start_time} 00:00:00' and '${start_time} 23:59:59';


select sleep(5) as sleep3;


-- 表：dwd_ais_vessel_port_all_info -- 船舶出发到达港口全量 acquire_timestamp_format
insert into sa.dwd_ais_vessel_port_all_info
select
    *
from doris_ecs.sa.dwd_ais_vessel_port_all_info
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(5) as sleep4;


-- 表：dws_ais_vessel_port_status_info -- 船舶出发到达港口状态  acquire_timestamp_format
insert into sa.dws_ais_vessel_port_status_info
select
    *
from doris_ecs.sa.dws_ais_vessel_port_status_info
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(5) as sleep5;


-- 表：dws_vessel_list_status_rt -- marinetraffic船舶状态数据 acquire_timestamp_format
insert into sa.dws_vessel_list_status_rt
select
    *
from doris_ecs.sa.dws_vessel_list_status_rt
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(5) as sleep6;


-- 表：dwd_ais_vessel_all_info -- 船舶全量表 update_time
insert into sa.dwd_ais_vessel_all_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_ais_vessel_all_info
where update_time between '${start_time} 00:00:00' and '${start_time} 12:00:00';


select sleep(10) as sleep7;


insert into sa.dwd_ais_vessel_all_info
select
    vessel_id
     ,acquire_timestamp_format
     ,acquire_timestamp
     ,vessel_name
     ,c_name
     ,imo
     ,mmsi
     ,callsign
     ,rate_of_turn
     ,orientation
     ,master_image_id
     ,lng
     ,lat
     ,source
     ,speed
     ,speed_km
     ,vessel_class
     ,vessel_class_name
     ,vessel_type
     ,vessel_type_name
     ,draught
     ,cn_iso2
     ,country_name
     ,nav_status
     ,nav_status_name
     ,dimensions_01
     ,dimensions_02
     ,dimensions_03
     ,dimensions_04
     ,block_map_index
     ,block_range_x
     ,block_range_y
     ,position_country_code2
     ,friend_foe
     ,sea_id
     ,sea_name
     ,update_time
from doris_ecs.sa.dwd_ais_vessel_all_info
where update_time between '${start_time} 12:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep8;


-- 表：dwd_vessel_list_all_rt -- marinetraffic船舶全量数据  acquire_timestamp_format
insert into sa.dwd_vessel_list_all_rt
select
    *
from doris_ecs.sa.dwd_vessel_list_all_rt
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep9;


-- 表：dwd_satellite_all_info -- 卫星 current_date
insert into sa.dwd_satellite_all_info
select
    *
from doris_ecs.sa.dwd_satellite_all_info
where current_date between to_date(days_sub('${start_time}',2)) and to_date(days_add('${start_time}',1));


select sleep(10) as sleep10;




select sleep(10) as sleep13;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据  acquire_timestamp_format
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between '${start_time} 00:00:00' and '${start_time} 08:00:00';


select sleep(20) as sleep14;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据  acquire_timestamp_format
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between '${start_time} 08:00:00' and '${start_time} 16:00:00';


select sleep(20) as sleep15;


-- 表：dwd_adsbexchange_aircraft_list_rt -- adsbexchange飞机全量数据  acquire_timestamp_format
insert into sa.dwd_adsbexchange_aircraft_list_rt
select
    *
from doris_ecs.sa.dwd_adsbexchange_aircraft_list_rt
where acquire_timestamp_format
between '${start_time} 16:00:00' and '${end_time} 00:00:00';


select sleep(20) as sleep16;


-- 表：dws_aircraft_combine_status_rt -- 飞机融合状态数据表  update_time
insert into sa.dws_aircraft_combine_status_rt
select
    *
from doris_ecs.sa.dws_aircraft_combine_status_rt
where update_time
between '${start_time} 00:00:00' and '${end_time} 00:00:00';


select sleep(10) as sleep17;


-- 表：dws_flight_segment_rt -- 飞机融合航班表 update_time
insert into sa.dws_flight_segment_rt
select
  *
from doris_ecs.sa.dws_flight_segment_rt
where update_time
between '${start_time} 00:00:00' and '${end_time} 00:00:00';

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"