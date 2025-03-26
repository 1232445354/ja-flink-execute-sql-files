#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dim_aircraft_id_track_cnt
select
        id,
        max_acquire_time,
        curr_cnt + yes_cnt as cnt,
        now() as update_time
from (
select
        t1.id,
        t1.curr_cnt,
        t1.max_acquire_time,
        if(t2.cnt is null,0,t2.cnt) as yes_cnt
from (
select
        flight_id as id,
        count(1) as curr_cnt,
        max(acquire_time) as max_acquire_time
from dwd_bhv_aircraft_combine_rt
where acquire_time >= '${start_time}'
  and acquire_time <'${end_time}'
group by flight_id
) as t1
left join sa.dim_aircraft_id_track_cnt as t2
on t1.id = t2.id
where t2.id is null
  or seconds_diff(t1.max_acquire_time,t2.max_acquire_time) > 0
) as tt
"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
