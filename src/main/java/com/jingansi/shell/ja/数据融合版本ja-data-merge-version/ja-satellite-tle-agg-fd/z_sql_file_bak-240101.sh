#!/bin/bash

start_day=${1}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
use sa;
insert into dws_satellite_tle_info
select
        satellite_no,
        to_date('${start_day}') as \`current_date\`,
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
        now() as update_time
from
        (
        select
                t1.*,
                row_number() over(partition by t1.satellite_no order by epoch_time desc,src_code asc) as rn
        from
                (
                select
                        *
                from
                        sa.dwd_satellite_tle_list
                where
                        epoch_time between date_sub('${start_day}',3) and date_add('${start_day}',1)
        union all
                select
                        *
        except(\`current_date\`)
        from
                dws_satellite_tle_info
        where
                \`current_date\` = to_date(date_sub('${start_day}',1))
                ) t1
        left join (select * from sa.dws_satellite_entity_info where object_status_code='D') t2
        on t1.satellite_no=t2.satellite_no
        where t2.decay_date is null or to_date('${start_day}')<t2.decay_date
) a
where
        rn = 1;


insert into sa.dws_satellite_entity_info(satellite_no,intl_code,satellite_name,friend_foe,classification,remark,update_time)
select
        t1.satellite_no,
        t1.international_designator,
        t1.satellite_name,
        'NEUTRALITY',
        t1.elset_classification,
        1 as remark,
        now() as update_time
from (
select
         satellite_no,
         \`current_date\`,
         satellite_name,
         elset_classification,  -- 秘密级别
         international_designator -- 国际编号
from sa.dws_satellite_tle_info
where \`current_date\` >= to_date(now())
) as t1
left join sa.dws_satellite_entity_info as t2
on t1.satellite_no = t2.satellite_no
where t2.satellite_no is null;



insert into sa.dws_satellite_image_info
select
        t1.satellite_no as id,
        now() as acquire_timestamp_format,
        '[{\"url\":\"/ja-acquire-images/ja-satellite-images/satellite.jpg\"}]' as image_url,
        null as remark,
        null as create_by,
        now() as update_time
        from (
select
         satellite_no,
        \`current_date\`,
         satellite_name
from sa.dws_satellite_tle_info
where \`current_date\` >= to_date(now())
) as t1
left join sa.dws_satellite_image_info as t2
on t1.satellite_no = t2.id
where t2.id is null;
"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
