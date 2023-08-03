--********************************************************************--
-- author:      write your name here
-- create time: 2023/4/15 17:37:58
-- description: write your description here
--********************************************************************--

set 'pipeline.name' = 'ja-ais-vessel-detail-single';
set 'table.exec.state.ttl' = '600000';
-- set 'parallelism.default' = '5';

-- set 'execution.checkpointing.interval' = '100000';
-- set 'state.checkpoints.dir' = 's3://flink/ja-ais-vessel-rt';
-- 空闲分区不用等待
-- set 'table.exec.source.idle-timeout' = '3s';



-- 创建kafka数据来源的表（Source：kafka）
drop table  if exists ais_detail_single_kafka;
create table ais_detail_single_kafka(
                                        vesselId             bigint       comment '船ID',
                                        `data` row(                                      -- 数据
                                            vessels array<                               -- 船
                                            row(
                                            myFleetInformation row(              -- 船队信息
                                            __typename          string,      -- 类型名称flag
                                            isOnMyFleet         boolean,     -- 是我的船队
                                            isOnSharedFleet     boolean,     -- 是否共享船队
                                            isOnOwnFleet        boolean      -- 是否拥有船队
                                            ),
                                            staticInformation row(               -- 静态信息
                                            eta                 bigint,      -- 预计到达时间，在军用词汇中经常用到
                                            draught             double,      -- 吃水深度
                                            __typename          string,      -- 类型名称flag
                                            destination         string,      -- 目的地
                                            `timestamp`         bigint       -- 船只时间
                                            ),
                                            `identity` row(                        -- 身份
                                            vesselId            bigint,      -- 船ID
                                            mmsi                bigint,      -- 船MMSI
                                            __typename          string,      -- 类型名称flag
                                            name                string,      -- 船名称
                                            callsign            string,      -- 呼号
                                            imo                 bigint       -- IMO
                                            ),
                                            __typename              string,      -- 类型名称flag
                                            navigationalInformation row(         -- 导航信息
                                            rateOfTurn          double,      -- 转向率
                                            heading             double,      -- 船首向
                                            __typename          string,      -- 类型名称
                                            courseOverGround    double,      -- 对地航向
                                            location row(
                                            longitude       double,      -- 导航经度
                                            latitude        double,      -- 导航纬度
                                            __typename      string,      -- 类型名称flag
                                            locationDescription array <  -- 位置描述
                                            row(
                                            __typename string,     -- 类型名称flag
                                            shortName  string      -- 短名称
                                            )
                                            >
                                            ),
                                            navigationalStatus       string,      -- 导航状态
                                            source                   string,      -- 导航数据来源
                                            speed                    double,      -- 导航速度
                                            `timestamp`              bigint       -- 导航时间戳
                                            ),
                                            datasheet row(                            -- 数据表
                                            owner                    string,      -- 所有者
                                            draughtAverage           double,      -- 吃水平均值
                                            grossTonnage             double,      -- 总吨数
                                            speedMax                 double,      -- 速度最大值
                                            __typename               string,      -- 类型名称flag
                                            length                   double,      -- 长度
                                            speedAverage             double,      -- 速度平均值
                                            flagCountryCode          string,      -- 标志国家代码
                                            yearBuilt                double,      -- 建成年份
                                            vesselClass              string,      -- 船类别
                                            riskRating               string,      -- 风险评级
                                            vesselType               string,      -- 船类型
                                            serviceStatus            string,      -- 服务状态
                                            width                    double,      -- 宽度
                                            deadweight               double,      -- 重物，载重吨位
                                            height                   double       -- 高度
                                            )
                                            )
                                            >
)
    ) with (
    'connector' = 'kafka',
    'topic' = 'ais_fleetmon_detail_single_item',
    'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
    'properties.group.id' = 'ais_fleetmon_detail_single_item-rt',
    -- 'scan.startup.mode' = 'latest-offset',
    'scan.startup.mode' = 'timestamp',
    'scan.startup.timestamp-millis' = '1681551705000',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);



-- 创建映射doris的船只的详细信息数据表(Sink:doris)
drop table  if exists ais_all_detail_info_doris;
create table ais_all_detail_info_doris(
                                          vessel_id      	 				    bigint		comment '船ID',
                                          fleet_typename 						string		comment '船队类型名称flag',
                                          is_on_my_fleet						boolean		comment '是我的船队',
                                          is_on_shared_fleet					boolean		comment '是否共享船队',
                                          is_on_own_fleet						boolean 	comment '是否拥有船队',
                                          eta                  				bigint   	comment '预计到达时间，在军用词汇中经常用到',
                                          draught							    string   	comment '吃水',
                                          static_typename						string		comment '静态类型名称flag',
                                          destination 						string		comment '目的地',
                                          static_timestamp					bigint 		comment '船只时间',
                                          vessel_id2							bigint 		comment '船ID',
                                          mmsi								string      comment 'mmsi',
                                          identity_typename					string		comment '身份类型名称flag',
                                          vessel_name 						string      comment '船名称2',
                                          callsign 							string      comment '呼号',
                                          imo      							string      comment 'IMO',
                                          vessels_typename					string		comment '船类型名称flag',
                                          rate_of_turn 						double		comment '转向率2',
                                          heading								double 		comment '船首向',
                                          navigational_typename				string		comment '导航类型名称flag',
                                          course_over_ground					double      comment '对地航向',
                                          longitude				            double 		comment '导航经度',
                                          latitude				            double 		comment '导航纬度',
                                          location_typename					string		comment '位置类型名称flag',
                                          location_description_typename		string		comment '位置描述类型名称flag',
                                          location_description_short_name		string		comment '位置短名称',
                                          navigational_status					string		comment '导航状态',
                                          navigational_source         		string      comment '导航数据来源',
                                          navigational_speed          		double      comment '导航速度',
                                          navigational_timestamp      		bigint      comment '导航时间戳',
                                          datasheet_owner             		string      comment '所有者',
                                          draught_average           			double      comment '吃水平均值',
                                          gross_tonnage             			double      comment '总吨数',
                                          speed_max                 			double      comment '速度最大值',
                                          datasheet_typename          		string      comment '类型名称flag',
                                          length                   			string		comment '长度',
                                          speed_average             			double      comment '速度平均值',
                                          flag_country_code          			string      comment '标志国家代码',
                                          year_built                			string      comment '建成年份',
                                          vessel_class              			string	 	comment '船类别',
                                          risk_rating               			string	 	comment '风险评级',
                                          vessel_type               			string	 	comment '船类型',
                                          service_status            			string	 	comment '服务状态',
                                          width                    			string      comment '宽度',
                                          deadweight               			double      comment '重物，载重吨位',
                                          height                   			string      comment '高度',
                                          ais_name                            string      comment 'ais名称',
                                          speed                               string      comment '速度',
                                          wail                                string      comment '',
                                          country                             string      comment '国家',
                                          update_time             			string      comment '数据入库时间'
)WITH (
     'connector' = 'doris',
     'fenodes' = '172.21.30.202:30030',
     'table.identifier' = 'kesadaran_situasional.ais_all_detail_info',
     'username' = 'admin',
     'password' = 'admin',
     'doris.request.tablet.size'='1',
     'doris.request.read.timeout.ms'='30000',
     'sink.batch.size'='100000',
     'sink.batch.interval'='5s',
     'sink.properties.escape_delimiters' = 'flase',
     'sink.properties.column_separator' = '\x01',	 -- 列分隔符
     'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
     'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
     );




-- 解析数据第1步
drop table if exists tem_ais_kafka_01;
create view tem_ais_kafka_01 as
select
    vesselId        as vessel_id,
    `data`.vessels[1] as tmp_object_data
from ais_detail_single_kafka
where vesselId is not null;



-- 解析数据第2步
drop table if exists tem_ais_kafka_02;
create view tem_ais_kafka_02 as
select
    vessel_id,
    tmp_object_data.myFleetInformation as my_fleet_information,
    tmp_object_data.staticInformation as static_information,
    tmp_object_data.`identity` as identity_information,
    tmp_object_data.__typename as vessels_typename,
        tmp_object_data.navigationalInformation as navigational_information,
        tmp_object_data.datasheet as datasheet
        from tem_ais_kafka_01
        where tmp_object_data is not null;


-- 解析数据第3步
drop table if exists tem_ais_kafka_03;
create view tem_ais_kafka_03 as
select
    vessel_id,
    my_fleet_information.__typename as fleet_typename,
        my_fleet_information.isOnMyFleet as is_on_my_fleet,
        my_fleet_information.isOnSharedFleet as is_on_shared_fleet,
        my_fleet_information.isOnOwnFleet as is_on_own_fleet,
        static_information.eta as eta,
        static_information.draught as draught2,
        static_information.__typename as static_typename,
        static_information.destination as destination,
        static_information.`timestamp` as static_timestamp,
        identity_information.vesselId as vessel_id2,
        identity_information.mmsi as mmsi,
        identity_information.__typename as identity_typename,
        identity_information.name as vessel_name2,
        identity_information.callsign as callsign,
        identity_information.imo as imo,
        vessels_typename,
        navigational_information.rateOfTurn as rate_of_turn2,
        navigational_information.heading as heading,
        navigational_information.__typename as navigational_typename,
        navigational_information.courseOverGround as course_over_ground,
        navigational_information.location.longitude as longitude,
        navigational_information.location.latitude as latitude,
        navigational_information.location.__typename as location_typename,
        navigational_information.location.locationDescription[1].__typename as location_description_typename,
        navigational_information.location.locationDescription[1].shortName as location_description_short_name,
        navigational_information.navigationalStatus as navigational_status,
        navigational_information.source as navigational_source,
        navigational_information.speed as navigational_speed,
        navigational_information.`timestamp` as navigational_timestamp,
        datasheet.owner as datasheet_owner,
        datasheet.draughtAverage as draught_average,
        datasheet.grossTonnage as gross_tonnage,
        datasheet.speedMax as speed_max,
        datasheet.__typename as datasheet_typename,
        datasheet.length as length,
        datasheet.speedAverage as speed_average,
        datasheet.flagCountryCode as flag_country_code,
        datasheet.yearBuilt as year_built,
        datasheet.vesselClass as vessel_class2,
        datasheet.riskRating as risk_rating,
        datasheet.vesselType as vessel_type,
        datasheet.serviceStatus as service_status,
        datasheet.width as width,
        datasheet.deadweight as deadweight,
        datasheet.height as height
        from tem_ais_kafka_02;



-- ais全量数据的详细信息入库
insert into ais_all_detail_info_doris
select
    vessel_id      	 					    ,
    fleet_typename 							,
    is_on_my_fleet							,
    is_on_shared_fleet						,
    is_on_own_fleet							,
    eta                  					,
    cast(draught2 as string)  as draught	,
    static_typename							,
    destination 							,
    static_timestamp						,
    vessel_id2					            ,
    cast(mmsi as string)	 as mmsi								,
    identity_typename						,
    vessel_name2 as vessel_name 			,
    callsign 								,
    cast(imo as string) as imo      								,
    vessels_typename						,
    rate_of_turn2 as rate_of_turn 			,
    heading									,
    navigational_typename					,
    course_over_ground						,
    longitude					            ,
    latitude					            ,
    location_typename						,
    location_description_typename			,
    location_description_short_name			,
    navigational_status						,
    navigational_source         			,
    navigational_speed          			,
    navigational_timestamp      			,
    datasheet_owner             			,
    draught_average           				,
    gross_tonnage             				,
    speed_max                 				,
    datasheet_typename          			,
    cast(length as string) as length                   				,
    speed_average             				,
    flag_country_code          				,
    cast(year_built as string) as year_built,
    vessel_class2 as vessel_class           ,
    risk_rating               				,
    vessel_type               				,
    service_status            				,
    cast(width as string) as width          ,
    deadweight               				,
    cast(height as string) as  height       ,
    cast(null as varchar) as ais_name,
    cast(null as varchar) as speed,
    cast(null as varchar) as wail,
    cast(null as varchar) as country,
    from_unixtime(unix_timestamp()) as update_time
from tem_ais_kafka_03;



