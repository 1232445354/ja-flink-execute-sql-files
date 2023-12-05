#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dws_aircraft_combine_list_rt
select * from doris_ecs.sa.dws_aircraft_combine_list_rt
where acquire_time > date_sub(now(),interval 3 minute);

insert into sa.dws_aircraft_combine_status_rt
select * from doris_ecs.sa.dws_aircraft_combine_status_rt
where acquire_time > date_sub(now(),interval 3 minute);

insert into sa.dws_flight_segment_rt
select * from doris_ecs.sa.dws_flight_segment_rt
where update_time > date_sub(now(),interval 2 minute);
"

echo -en "开始同步数据...$(date)\n"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"

echo -en "数据同步SUCCESS.......$(date)\n"
echo -en "------------------------------------------\n"

