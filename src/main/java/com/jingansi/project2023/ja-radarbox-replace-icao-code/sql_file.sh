#!/bin/bash

current_minute=${1}
last1hour_minute=${2}

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

echo -en "共9步...."
echo -en "1. 清空mysql的临时表temp_radarbox_aircraft_mysql_01 \n"

truncate_mysql_table="
truncate table ja_sa.temp_radarbox_aircraft_mysql_01;
"
mysql -h${host_mysql} \
-P${port_mysql} \
-u${username_mysql} \
-p${password_mysql} \
-e "${truncate_mysql_table}"


echo -en "2. 清空doris的临时表temp_radarbox_aircraft_01\n"
echo -en "3. 清空doris的临时表temp_radarbox_aircraft_02\n"
echo -en "4. 筛选数据针对s模式\n"
echo -en "5. 将数据进行关联,关联数据插回到原轨迹表中\n"
echo -en "6. 将对应关系表同步到mysql中\n"


sql1="

truncate table sa.temp_radarbox_aircraft_01;

truncate table sa.temp_radarbox_aircraft_02;

-- 将s模式为null的并且存在可以换的追踪id的数据插入tem01
insert into sa.temp_radarbox_aircraft_01
select
	*
from sa.dws_aircraft_combine_list_rt
where flight_id in(
	select
    src_pk
  from sa.dws_aircraft_combine_list_rt
  where acquire_time between '${last1hour_minute}' and '${current_minute}'
    and src_code = 1
  group by src_pk
  having count(distinct flight_id) > 1
)
and acquire_time between '${last1hour_minute}' and '${current_minute}'
and src_code = 1;


-- 对应关系表
insert into sa.temp_radarbox_aircraft_02
select
	flight_id,
  src_pk,
  max_by(registration,acquire_time) as registration,
	max_by(is_military,acquire_time) as is_military,
	max_by(flight_photo,acquire_time) as flight_photo,
	max_by(country_code,acquire_time) as country_code,
	max_by(country_name,acquire_time) as country_name
from sa.dws_aircraft_combine_list_rt
where acquire_time between '${last1hour_minute}' and '${current_minute}'
 and src_code = 1
 and icao_code is not null
 and src_pk in (
	select flight_id from sa.temp_radarbox_aircraft_01 group by flight_id
)
group by flight_id,src_pk;


 -- 插入全量数据表
 insert into sa.dws_aircraft_combine_list_rt
 	select
 	t2.icao_code as flight_id
 	,t1.acquire_time
 	,t1.src_code
 	,t2.icao_code as icao_code
	,if(t1.registration = '' or t1.registration is null,t2.registration,t1.registration) as registration
 	,t1.flight_no
 	,t1.callsign
 	,t1.flight_type
 	,if(t1.is_military is null,t2.is_military,t1.is_military) as is_military
 	,'hex' as pk_type
 	,t1.flight_id as src_pk
 	,t1.flight_category
 	,t1.flight_category_name
 	,t1.lng
 	,t1.lat
 	,t1.speed
 	,t1.speed_km
 	,t1.altitude_baro
 	,t1.altitude_baro_m
 	,t1.altitude_geom
 	,t1.altitude_geom_m
 	,t1.heading
 	,t1.squawk_code
 	,t1.flight_status
 	,t1.special
 	,t1.origin_airport3_code
 	,t1.origin_airport_e_name
 	,t1.origin_airport_c_name
 	,t1.origin_lng
 	,t1.origin_lat
 	,t1.dest_airport3_code
 	,t1.dest_airport_e_name
 	,t1.dest_airport_c_name
 	,t1.dest_lng
 	,t1.dest_lat
 	,if(t1.flight_photo = '' or t1.flight_photo is null,t2.flight_photo,t1.flight_photo) as flight_photo
 	,t1.flight_departure_time
 	,t1.expected_landing_time
 	,t1.to_destination_distance
 	,t1.estimated_landing_duration
 	,t1.airlines_icao
 	,t1.airlines_e_name
 	,t1.airlines_c_name
 	,if(t1.country_code = '' or t1.country_code is null,t2.country_code,t1.country_code) as country_code
 	,if(t1.country_name = '' or t1.country_name is null,t2.country_name,t1.country_name) as country_name
 	,t1.data_source
 	,t1.source
 	,t1.position_country_code2
 	,t1.position_country_name
 	,t1.friend_foe
 	,t1.sea_id
 	,t1.sea_name
 	,t1.h3_code
 	,t1.extend_info
 	,t1.update_time
 from sa.temp_radarbox_aircraft_01 as t1
 inner join sa.temp_radarbox_aircraft_02 as t2
 on t1.flight_id = t2.src_pk;


-- 将对应关系表同步到mysql中
insert into mysql.ja_sa.temp_radarbox_aircraft_mysql_01
select
  icao_code,src_pk
from sa.temp_radarbox_aircraft_02;

"

mysql -h${host_doris} \
-P${port_doris} \
-u${username_doris} \
-p${password_doris} \
-e "${sql1}"



echo -en "7. 删除融合轨迹表关联上的这一部分数据 \n"


query_delete_sql="

use sa;
select
	distinct t1.flight_id as flight_id
	-- t1.acquire_time
from sa.temp_radarbox_aircraft_01 as t1
inner join sa.temp_radarbox_aircraft_02 as t2
on t1.flight_id = t2.src_pk;

"

count=0
# 连接数据库将需要删除的数据写出成文件
mysql -h${host_doris} \
-P${port_doris} \
-u${username_doris} \
-p${password_doris} \
-s -e "${query_delete_sql}" > flight_id_nos.txt


# 再次遍历文件，将数据取出，进行删除
flight_id_nos=""
while read flight_id_no ;
do
	flight_id_nos="${flight_id_nos},'${flight_id_no}'"
	((count++))
  if [ $((count % 1000)) -eq 0 ]; then
  	echo -en '数据id=1000，进行删除操作\n'
    a="delete from sa.dws_aircraft_combine_list_rt where flight_id in (${flight_id_nos:1}) and acquire_time >= '${last1hour_minute}' and acquire_time <= '${current_minute}'"
    mysql -h${host_doris} -P${port_doris} -u${username_doris} -p${password_doris}  -e "${a}"
    flight_id_nos=""
  fi

done < flight_id_nos.txt

if [ -n "$flight_id_nos" ]; then
  echo -en "flight_id_nos 不为空 执行剩余的删除操作\n"
  a="delete from sa.dws_aircraft_combine_list_rt where flight_id in (${flight_id_nos:1}) and acquire_time >= '${last1hour_minute}' and acquire_time <= '${current_minute}'"
  mysql -h${host_doris} -P${port_doris} -u${username_doris} -p${password_doris} -e "${a} "
else
  echo -en "flight_id_nos 为空,不用进行删除了\n"
fi


echo -en "8. 更新mysql中的昵称表\n"
echo -en "9. 更新mysql中的关注目标表\n"

nickname_follow_sql="
use ja_sa;

-- 两个的昵称，查询出需要删除的
insert into ja_sa.temp_radarbox_aircraft_mysql_02
select
	*
from ja_sa.entity_custom_info
where (target_id,username) in(
select
  t11.src_pk,
	-- t11.icao_code,
	t22.username
from ja_sa.temp_radarbox_aircraft_mysql_01 as t11
inner join (
select
  t2.icao_code,
  t1.username,
  count(1) as cnt
from (select * from ja_sa.entity_custom_info where target_type = 'AIRCRAFT') as t1
inner join ja_sa.temp_radarbox_aircraft_mysql_01 as t2
on t1.target_id = t2.src_pk
or t1.target_id = t2.icao_code
group by t1.username,t2.icao_code

having cnt > 1)
 as t22
on t11.icao_code = t22.icao_code
);


-- 两个的昵称删除操作
delete from ja_sa.entity_custom_info
 where id in(select id from ja_sa.temp_radarbox_aircraft_mysql_02);


-- 一个的昵称替换操作
replace into ja_sa.entity_custom_info
select
  t1.id
  ,t1.situation_id
  ,t2.icao_code as target_id
  ,t1.target_type
  ,t1.type
  ,t1.username
	,t1.nickname
  ,t1.remark
  ,t1.gmt_create
  ,t1.gmt_modified
  ,t1.deleted
  ,t1.gmt_create_by
  ,t1.gmt_modified_by
from (select * from ja_sa.entity_custom_info where target_type = 'AIRCRAFT') as t1
inner join ja_sa.temp_radarbox_aircraft_mysql_01 as t2
on t1.target_id = t2.src_pk;



-- 关注两个飞机目标，查询出需要删除的
insert into ja_sa.temp_radarbox_aircraft_mysql_03
select
	*
from ja_sa.follow_target
where (target_id,username) in(
select
  t11.src_pk,
	-- t11.icao_code,
	t22.username
from ja_sa.temp_radarbox_aircraft_mysql_01 as t11
inner join (
select
  t2.icao_code,
  t1.username,
  count(1) as cnt
from (select * from ja_sa.follow_target where target_type = 'AIRCRAFT') as t1
inner join ja_sa.temp_radarbox_aircraft_mysql_01 as t2
on t1.target_id = t2.src_pk
or t1.target_id = t2.icao_code
group by t1.username,t2.icao_code
having cnt > 1)
 as t22
on t11.icao_code = t22.icao_code
);


-- 两个的关注目标删除一个
delete from ja_sa.follow_target
where id in (select id from ja_sa.temp_radarbox_aircraft_mysql_03);


-- 一个的关注目标替换
replace into ja_sa.follow_target
select
	 tt1.id
	,tt1.target_id
	,coalesce(tt2.nickname,tt1.target_id) target_name
	,tt1.target_type
	,tt1.username
	,tt1.gmt_create
	,tt1.gmt_modified
	,tt1.gmt_create_by
	,tt1.gmt_create_by_name
	,tt1.gmt_modified_by
	,tt1.gmt_modified_by_name
	,tt1.follow_group_id
from (
select
	id
	,if(t2.icao_code is not null,t2.icao_code,t1.target_id) as target_id
	,t1.target_name
	,t1.target_type
	,t1.username
	,t1.gmt_create
	,t1.gmt_modified
	,t1.gmt_create_by
	,t1.gmt_create_by_name
	,t1.gmt_modified_by
	,t1.gmt_modified_by_name
	,t1.follow_group_id
from (
select * from ja_sa.follow_target where target_type = 'AIRCRAFT') as t1
left join ja_sa.temp_radarbox_aircraft_mysql_01 as t2  -- innser join 有问题
on t1.target_id = t2.src_pk
) as tt1
left join ja_sa.entity_custom_info as tt2
on tt1.target_id = tt2. target_id
and tt1.username = tt2.username;

"

mysql -h${host_mysql} \
-P${port_mysql} \
-u${username_mysql} \
-p${password_mysql} \
-e "${nickname_follow_sql}"

