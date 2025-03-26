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

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
