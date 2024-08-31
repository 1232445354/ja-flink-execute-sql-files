#!/bin/bash

start_time=${1}
end_time=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="


insert into sa.dws_ais_landbased_vessel_status(mmsi,acquire_time,imo,ship_name,length,width,call_no,ship_and_carg_type,update_time)
select
	mmsi,
	acquire_time,
	 nullif(line_arr[1],'') as imo,
	 nullif(line_arr[2],'') as ship_name,
	 nullif(line_arr[3],'') as length,
	 nullif(line_arr[4],'') as width,
	 nullif(line_arr[5],'') as call_no,
	 nullif(line_arr[6],'') as ship_and_carg_type,
	 now() as update_time
from (
select
	mmsi,
	max(acquire_time) as acquire_time,
	split_by_string(max_by(line,acquire_time) ,'¥') as line_arr
from (
select
	mmsi,
	acquire_time,
	length,
	width,
	concat(
	ifnull(if(imo is null or imo = '0','',imo),''),'¥',
	ifnull(ship_name,''),'¥',
	ifnull(if(length is null or length = '0','',length),''),'¥',
	ifnull(if(width is null or width = '0','',width),''),'¥',
	ifnull(call_no,''),'¥',
	ifnull(ship_and_carg_type,'')
	) as line
from sa.dwd_ais_landbased_vessel_list
 where acquire_time between '${start_time}' and '${end_time}'
) as t1
group by mmsi
) as t2

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"
