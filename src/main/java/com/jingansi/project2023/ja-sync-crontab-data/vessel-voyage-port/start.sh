#!/bin/bash
mysql -h172.21.30.202 -P31030 -uadmin -pJingansi@110  -e "
  insert into sa.dwd_ais_vessel_port_all_info
  select * from doris_ecs.sa.dwd_ais_vessel_port_all_info
  where acquire_timestamp_format > DATE_SUB(now(),interval 5 hour);

  insert into sa.dws_ais_vessel_port_status_info
  select * from doris_ecs.sa.dws_ais_vessel_port_status_info
  where acquire_timestamp_format > DATE_SUB(now(),interval 5 hour)

"
echo $(date)
echo "执行完成....."

