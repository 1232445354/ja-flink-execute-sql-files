#!/bin/bash

1. 清空mysql的临时表
2. 清空doris的临时表temp_radarbox_aircraft_01
3. 清空doris的临时表temp_radarbox_aircraft_02
4. 筛选数据针对s模式
5. 将数据进行关联,插入到轨迹表中
6. 将对应关系表同步到mysql中
7. 删除融合轨迹表关联上的这一部分数据
8. 更新mysql中的昵称表
9. 更新mysql中的关注目标表


# *******************************************

# 部署:172.21.30.105

# *******************************************
  /data1/bigdata/apps/ja-radarbox-replace-icao-code

启动:
*/5 * * * *  sh /data1/bigdata/apps/ja-radarbox-replace-icao-code/start.sh > /data1/bigdata/apps/ja-radarbox-replace-icao-code/root.log &



# *******************************************

# 部署:172.21.30.201

# *******************************************
  /data1/bigdata/apps/ja-radarbox-replace-icao-code

启动:
*/5 * * * *  sh /data1/bigdata/apps/ja-radarbox-replace-icao-code/start.sh > /data1/bigdata/apps/ja-radarbox-replace-icao-code/root.log &



# *******************************************

# 部署:server-1

# *******************************************
  /data1/bigdata/apps/ja-radarbox-replace-icao-code

启动:
*/5 * * * *  sh /data1/bigdata/apps/ja-radarbox-replace-icao-code/start.sh > /data1/bigdata/apps/ja-radarbox-replace-icao-code/root.log &


