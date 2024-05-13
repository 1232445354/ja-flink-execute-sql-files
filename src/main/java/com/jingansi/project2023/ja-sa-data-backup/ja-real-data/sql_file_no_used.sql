-- 船舶出发到达港口全量表 acquire_timestamp_format
insert into sa.dwd_ais_vessel_port_all_info
select
    *
from doris_ecs.sa.dwd_ais_vessel_port_all_info
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';



-- 船舶出发到达港口状态  acquire_timestamp_format
insert into sa.dws_ais_vessel_port_status_info
select
    *
from doris_ecs.sa.dws_ais_vessel_port_status_info
where acquire_timestamp_format between '${start_time} 00:00:00' and '${end_time} 00:00:00';


-- 卫星实体表 update_time
insert into sa.dws_satellite_entity_info
select
    *
from doris_ecs.sa.dws_satellite_entity_info
where update_time between to_date(days_sub('${start_time}',2)) and '${end_time} 00:00:00';

