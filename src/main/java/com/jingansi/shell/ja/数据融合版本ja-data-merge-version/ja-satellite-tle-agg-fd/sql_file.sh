#!/bin/bash

start_day=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="

insert into sa.dws_bhv_satellite_list_fd
select
	id,
	 to_date('${start_day}') as today_time,
   satellite_no,
   src_code,
   epoch_time,
   acquire_time,
   satellite_name,
   line1,
   line2,
   one_line_no,
   elset_classification,
   international_designator,
   utc,
   mean_motion_1st_derivative,
   mean_motion_2nd_derivative,
   bstar_drag_term,
   element_set_type,
   element_number,
   checksum1,
   two_line_no,
   satellite_no2,
   orbit_inclination,
   right_ascension_ascending_node,
   eccentricity,
   argument_perigee,
   mean_anomaly,
   mean_motion,
   revolution_epoch_number,
   checksum2,
   object_status_code,
   object_status_name,
   decay_date,
   now() as update_time
from(
   select
      t1.*,
      if(decay_date <= '${start_day}','D',null) as object_status_code,
      if(decay_date <= '${start_day}','衰败',null) as object_status_name,
      decay_date,
      row_number() over(partition by t1.id order by epoch_time desc,src_code asc) as rn
		from(
        select
					satellite_no as id,
					*
         from sa.dwd_bhv_satellite_rt
         where epoch_time between date_sub('${start_day}',3) and date_add('${start_day}',1)

				union all

         select
					*
					except(today_time,object_status_code,object_status_name,decay_date)
				from sa.dws_bhv_satellite_list_fd
				where today_time = to_date(date_sub('${start_day}',1))
		) t1
      left join sa.dws_et_satellite_info as t2
      on t1.id=t2.id
) a
where rn = 1;


insert into sa.dws_et_satellite_info(
id,acquire_time,satellite_no,intl_code,e_name,classification,remark,friend_foe,search_content,update_time
)
select
      t1.id,
			now() as acquire_time,
			t1.id as satellite_no,
      t1.international_designator as intl_code,
      t1.e_name,
      t1.elset_classification as classification,
      1 as remark,
			'4' as friend_foe,
			concat(
				ifnull(t1.id,''),' ',
				ifnull(t1.international_designator,''),' ',
				ifnull(t1.e_name,'')
				) as search_content,
        now() as update_time
from (
select
         id,
         today_time,
         e_name,
         elset_classification,   -- 秘密级别
         international_designator -- 国际编号
from sa.dws_bhv_satellite_list_fd
where today_time >= to_date(now())
) as t1
left join sa.dws_et_satellite_info as t2
on t1.id = t2.id
where t2.id is null;


insert into sa.dws_satellite_image_info
select
    t1.id,
    now() as acquire_time,
    '[{\"url\":\"/ja-acquire-images/ja-satellite-images/satellite.jpg\"}]' as image_url,
    null as remark,
    null as create_by,
    now() as update_time
from (
select
     id,
		 today_time,
     e_name
from sa.dws_bhv_satellite_list_fd
where today_time >= to_date(now())
) as t1
left join sa.dws_atr_satellite_image_info as t2
on t1.id = t2.id
where t2.id is null;

update ja_argo.dws_et_argo_info
set `status` = t2.status_code,
country_code = t2.country_code,
country_name = t2.country_name
from ja_argo.dws_atr_argo_entity_info as t2
where ja_argo.dws_et_argo_info.id = t2.id;

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
