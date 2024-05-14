#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
-- 1. 合并marinetraffic和fleetmon数据
-- 数据入解绑记录表
insert into sa.dwd_vessel_id_modify_list
select
	vessel_id,
  'mtf' as src_code,
	t2_vessel_id as new_id,
  to_date(now()) as dt,
	now() as update_time
from (
select
	t1.*,
	t2.vessel_id as t2_vessel_id,
	t2.imo as t2_imo,
	t2.mmsi as t2_mmsi,
	t2.callsign as t2_callsign,
	t2.year_built as t2_year_built,
	t2.vessel_type as t2_vessel_type,
	t2.vessel_type_name as t2_vessel_type_name,
	t2.vessel_class  as t2_vessel_class,
	t2.vessel_class_name as t2_vessel_class_name,
	t2.name as t2_name,
	t2.c_name as t2_c_name,
	t2.length as t2_length,
	t2.width as t2_width,
	t2.height as t2_height,
	t2.owner as t2_owner,
	t2.service_status as t2_service_status,
	t2.service_status_name as t2_service_status_name,
	t2.flag_country_code as t2_flag_country_code,
	t2.country_name as t2_country_name,
	t2.source as t2_source,
	t3.acquire_timestamp_format,
	row_number()over(partition by t1.vessel_id order by t3.acquire_timestamp_format desc) as rk
from (
select
	*
from sa.dws_ais_vessel_detail_static_attribute
where source = 2
and mmsi is not null
and vessel_id not in (select ship_id + 1000000000 as ship_id from sa.dim_mt_fm_id_relation)
and mmsi not in (select mmsi from sa.dwd_vessel_mmsi_blacklist)
and vessel_id >= 1000000000
) as t1

left join (

select
	*
from sa.dws_ais_vessel_detail_static_attribute
where mmsi is not null
	and source = 'FLEETMON'

) as t2
on t1.mmsi = t2.mmsi

left join sa.dws_ais_vessel_status_info as t3
on t2.vessel_id = t3.vessel_id
where t2.vessel_id is not null

) as tt
where rk = 1;



-- 数据入对应关系表
insert into sa.dim_mt_fm_id_relation(ship_id,vessel_id,update_time)
select
	vessel_id - 1000000000 as ship_id,
	t2_vessel_id as vessel_id,
	now() as update_time
from (
select
	t1.*,
	t2.vessel_id as t2_vessel_id,
	t2.imo as t2_imo,
	t2.mmsi as t2_mmsi,
	t2.callsign as t2_callsign,
	t2.year_built as t2_year_built,
	t2.vessel_type as t2_vessel_type,
	t2.vessel_type_name as t2_vessel_type_name,
	t2.vessel_class  as t2_vessel_class,
	t2.vessel_class_name as t2_vessel_class_name,
	t2.name as t2_name,
	t2.c_name as t2_c_name,
	t2.length as t2_length,
	t2.width as t2_width,
	t2.height as t2_height,
	t2.owner as t2_owner,
	t2.service_status as t2_service_status,
	t2.service_status_name as t2_service_status_name,
	t2.flag_country_code as t2_flag_country_code,
	t2.country_name as t2_country_name,
	t2.source as t2_source,
	t3.acquire_timestamp_format,
	row_number()over(partition by t1.vessel_id order by t3.acquire_timestamp_format desc) as rk
from (
select
	*
from sa.dws_ais_vessel_detail_static_attribute
where source = 2
and mmsi is not null
and vessel_id not in (select ship_id + 1000000000 as ship_id from sa.dim_mt_fm_id_relation)
and mmsi not in (select mmsi from sa.dwd_vessel_mmsi_blacklist)
and vessel_id >= 1000000000
) as t1

left join (

select
	*
from sa.dws_ais_vessel_detail_static_attribute
where mmsi is not null
	and source = 'FLEETMON'

) as t2
on t1.mmsi = t2.mmsi

left join sa.dws_ais_vessel_status_info as t3
on t2.vessel_id = t3.vessel_id
where t2.vessel_id is not null

) as tt
where rk = 1;


-- 新入的mmsi也需要填入到vt对应关系表中
insert into sa.dim_mtf_vt_reletion_info
select
	vessel_id,
	null as mtf_mmsi,
	null as mtf_imo,
	null as mtf_callsign,
	null as mtf_name,
	mmsi as vt_mmsi,
	null as vt_imo,
	null as vt_callsign,
	null as vt_name,
	to_date(now()) as merge_way,
	now() as update_time
from (
select
	t1.vessel_id,
	t1.mmsi,
	row_number()over(partition by t1.vessel_id order by t2.acquire_timestamp_format desc) as rk
from (
select
	*
from sa.dws_ais_vessel_detail_static_attribute
where update_time > date_sub(now(),interval 2 day)
	and source in ('2','FLEETMON')
	and mmsi is not null
	and mmsi not in (select vt_mmsi from sa.dim_mtf_vt_reletion_info)
) as t1
left join sa.dws_ais_vessel_status_info as t2
on t1.vessel_id = t2.vessel_id
) as tt
where rk = 1;

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"