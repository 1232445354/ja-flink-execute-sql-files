

--********************************************************************--
-- author:      write your name here
-- create time: 2025/7/18 19:49:41
-- description: write your description here
--********************************************************************--
set 'pipeline.name' = 'ja-sync-kafka-data-rt';


CREATE TABLE OPEN_API_DEVICE_EVENT (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_DEVICE_EVENT',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '0',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


CREATE TABLE OPEN_API_ACTION_ITEM_RECORD_EVENT (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_ITEM_RECORD_EVENT',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


CREATE TABLE OPEN_API_ACTION_EVENTS_EVENT (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_EVENTS_EVENT',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      -- 'scan.startup.mode' = 'group-offsets',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


create table `groups` (
                          group_parent_id                string        comment '',
                          group_name                     string        comment '',
                          group_id                       string        comment '',
                          group_order                    int           comment '',
                          PRIMARY KEY (group_id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'groups',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );


-- 建立映射mysql的表（为了查询组织id）
create table users (
                       user_id	    int,
                       username	string,
                       password	string,
                       name	    string,
                       group_id	string,
                       primary key (user_id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/ja-4a?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'users',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '3600s',
     'lookup.max-retries' = '10'
     );


-- 行动任务表
create table action_item (
                             id                bigint, -- 子任务ID
                             action_id         bigint, -- 行动任务ID
                             status            string, -- 子任务状态，PENDING：未开始,PROCESSING：行动中，FINISHED：已完成
                             gmt_create_by	    string, -- 设备ID
                             primary key (id) NOT ENFORCED
)with (
     'connector' = 'jdbc',
     'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/chingchi-icos?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
     'username' = 'root',
     'password' = 'jingansi110',
     'table-name' = 'action_item',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '5000',
     'lookup.cache.ttl' = '20s',
     'lookup.max-retries' = '10'
     );


-- 子父设备表（Source：mysql）
create table iot_device (
                            id                             int           comment 'id',
                            org_code                      string        comment '组织 id',
                            device_id                      string        comment '子设备id',
                            PRIMARY KEY (id) NOT ENFORCED
) with (
      'connector' = 'jdbc',
      -- 'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'url' = 'jdbc:mysql://mysql57-mysql.base.svc.cluster.local:3306/dushu-v3?useSSL=false&characterEncoding=UTF-8&serverTimezone=GMT%2B8&autoReconnect=true',
      'driver' = 'com.mysql.cj.jdbc.Driver',
      'username' = 'root',
      'password' = 'jingansi110',
      'table-name' = 'iot_device',
      'lookup.cache.max-rows' = '5000',
      'lookup.cache.ttl' = '3600s',
      'lookup.max-retries' = '10'
      );



CREATE TABLE OPEN_API_DEVICE_EVENT_90046 (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_DEVICE_EVENT_90046',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


CREATE TABLE OPEN_API_ACTION_ITEM_RECORD_EVENT_90046 (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_ITEM_RECORD_EVENT_90046',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


CREATE TABLE OPEN_API_ACTION_EVENTS_EVENT_90046 (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_EVENTS_EVENT_90046',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );

CREATE TABLE OPEN_API_ACTION_EVENTS_EVENT_ZH (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_EVENTS_EVENT_ZH',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );

CREATE TABLE OPEN_API_DEVICE_EVENT_ZH (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_DEVICE_EVENT_ZH',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );


CREATE TABLE OPEN_API_ACTION_ITEM_RECORD_EVENT_ZH  (
    line STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'OPEN_API_ACTION_ITEM_RECORD_EVENT_ZH ',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      'properties.group.id' = 'ja-sync-kafka-data-rt',
      'format' = 'raw',
      'raw.charset' = 'UTF-8'
      );

create view tmp_01 as
select
    JSON_VALUE(line,'$.data.deviceId') as device_id,
    line,
    PROCTIME()  as proctime
from OPEN_API_DEVICE_EVENT;


create view tmp_02 as
select
    cast(JSON_VALUE(line,'$.data.actionItemId') as bigint) as actionItemId,
    line,
    PROCTIME()  as proctime
from OPEN_API_ACTION_ITEM_RECORD_EVENT;

create view tmp_03 as
select
    cast(JSON_VALUE(line,'$.data.actionItemId') as bigint) as actionItemId,
    line,
    PROCTIME()  as proctime
from OPEN_API_ACTION_EVENTS_EVENT;


begin statement set;


insert into OPEN_API_DEVICE_EVENT_90046
select
    line
from tmp_01 t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.device_id=t2.device_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on t2.org_code=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t2.org_code='90046'
   or t3.group_id='90046'
   or t4.group_id='90046'
   or t5.group_id='90046'
   or t6.group_id='90046'
   or t7.group_id='90046'
   or t8.group_id='90046'
   or t9.group_id='90046'
   or t10.group_id='90046'
   or t11.group_id='90046'
   or t12.group_id='90046'
   or t13.group_id='90046'
   or t14.group_id='90046'
   or t14.group_parent_id='90046'
;





insert into OPEN_API_ACTION_ITEM_RECORD_EVENT_90046
select
    line
from tmp_02 t1
         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.actionItemId=t2.id
         left join `users` FOR SYSTEM_TIME AS OF t1.proctime as u        -- 关联父子设备表,取出父设备ID
                   on t2.gmt_create_by=u.username
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on u.group_id=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t3.group_id='90046'
   or t4.group_id='90046'
   or t5.group_id='90046'
   or t6.group_id='90046'
   or t7.group_id='90046'
   or t8.group_id='90046'
   or t9.group_id='90046'
   or t10.group_id='90046'
   or t11.group_id='90046'
   or t12.group_id='90046'
   or t13.group_id='90046'
   or t14.group_id='90046'
   or t14.group_parent_id='90046'
;


insert into OPEN_API_ACTION_EVENTS_EVENT_90046
select
    line
from tmp_03 t1
         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.actionItemId=t2.id
         left join `users` FOR SYSTEM_TIME AS OF t1.proctime as u        -- 关联父子设备表,取出父设备ID
                   on t2.gmt_create_by=u.username
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on u.group_id=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t3.group_id='90046'
   or t4.group_id='90046'
   or t5.group_id='90046'
   or t6.group_id='90046'
   or t7.group_id='90046'
   or t8.group_id='90046'
   or t9.group_id='90046'
   or t10.group_id='90046'
   or t11.group_id='90046'
   or t12.group_id='90046'
   or t13.group_id='90046'
   or t14.group_id='90046'
   or t14.group_parent_id='90046'
;

insert into OPEN_API_DEVICE_EVENT_ZH
select
    line
from tmp_01 t1
         left join iot_device FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.device_id=t2.device_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on t2.org_code=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t2.org_code='90037'
   or t3.group_id='90037'
   or t4.group_id='90037'
   or t5.group_id='90037'
   or t6.group_id='90037'
   or t7.group_id='90037'
   or t8.group_id='90037'
   or t9.group_id='90037'
   or t10.group_id='90037'
   or t11.group_id='90037'
   or t12.group_id='90037'
   or t13.group_id='90037'
   or t14.group_id='90037'
   or t14.group_parent_id='90037'
;





insert into OPEN_API_ACTION_ITEM_RECORD_EVENT_ZH
select
    line
from tmp_02 t1
         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.actionItemId=t2.id
         left join `users` FOR SYSTEM_TIME AS OF t1.proctime as u        -- 关联父子设备表,取出父设备ID
                   on t2.gmt_create_by=u.username
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on u.group_id=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t3.group_id='90037'
   or t4.group_id='90037'
   or t5.group_id='90037'
   or t6.group_id='90037'
   or t7.group_id='90037'
   or t8.group_id='90037'
   or t9.group_id='90037'
   or t10.group_id='90037'
   or t11.group_id='90037'
   or t12.group_id='90037'
   or t13.group_id='90037'
   or t14.group_id='90037'
   or t14.group_parent_id='90037'
;


insert into OPEN_API_ACTION_EVENTS_EVENT_ZH
select
    line
from tmp_03 t1
         left join action_item FOR SYSTEM_TIME AS OF t1.proctime as t2        -- 关联父子设备表,取出父设备ID
                   on t1.actionItemId=t2.id
         left join `users` FOR SYSTEM_TIME AS OF t1.proctime as u        -- 关联父子设备表,取出父设备ID
                   on t2.gmt_create_by=u.username
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t3        -- 关联父子设备表,取出父设备ID
                   on u.group_id=t3.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t4        -- 关联父子设备表,取出父设备ID
                   on t3.group_parent_id=t4.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t5        -- 关联父子设备表,取出父设备ID
                   on t4.group_parent_id=t5.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t6        -- 关联父子设备表,取出父设备ID
                   on t5.group_parent_id=t6.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t7        -- 关联父子设备表,取出父设备ID
                   on t6.group_parent_id=t7.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t8        -- 关联父子设备表,取出父设备ID
                   on t7.group_parent_id=t8.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t9        -- 关联父子设备表,取出父设备ID
                   on t8.group_parent_id=t9.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t10        -- 关联父子设备表,取出父设备ID
                   on t9.group_parent_id=t10.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t11        -- 关联父子设备表,取出父设备ID
                   on t10.group_parent_id=t11.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t12        -- 关联父子设备表,取出父设备ID
                   on t11.group_parent_id=t12.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t13        -- 关联父子设备表,取出父设备ID
                   on t12.group_parent_id=t13.group_id
         left join `groups` FOR SYSTEM_TIME AS OF t1.proctime as t14        -- 关联父子设备表,取出父设备ID
                   on t13.group_parent_id=t14.group_id
where t3.group_id='90037'
   or t4.group_id='90037'
   or t5.group_id='90037'
   or t6.group_id='90037'
   or t7.group_id='90037'
   or t8.group_id='90037'
   or t9.group_id='90037'
   or t10.group_id='90037'
   or t11.group_id='90037'
   or t12.group_id='90037'
   or t13.group_id='90037'
   or t14.group_id='90037'
   or t14.group_parent_id='90037'
;

end;
