--********************************************************************--
-- author:      write your name here
-- create time: 2024/10/31 09:51:42
-- description: 哨兵2采集
--********************************************************************--
set 'pipeline.name' = 'ja-sentinel2-list';

-- set 'parallelism.default' = '2';
-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '300000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-sentinel2-list';


 -- -----------------------

 -- 数据结构

 -- -----------------------


create table sentinel2_list_kafka(
                                     id                                      string,
                                     photograph_datetime                     string,
                                     type                                    string,
                                     stac_version                            string,
                                     context                                 string,
                                     features_type                           string,
                                     created                                 string,
                                     platform                                string,
                                     constellation                           string,
                                     instruments                             string,
                                     eo_cloud_cover                          double,
                                     proj_epsg                               double,
                                     mgrs_utm_zone                           double,
                                     mgrs_latitude_band                      string,
                                     mgrs_grid_square                        string,
                                     grid_code                               string,
                                     view_sun_azimuth                        double,
                                     view_sun_elevation                      double,
                                     s2_degraded_msi_data_percentage         double,
                                     s2_nodata_pixel_percentage              double,
                                     s2_saturated_defective_pixel_percentage double,
                                     s2_cloud_shadow_percentage              double,
                                     s2_vegetation_percentage                double,
                                     s2_not_vegetated_percentage             double,
                                     s2_water_percentage                     double,
                                     s2_unclassified_percentage              double,
                                     s2_medium_proba_clouds_percentage       double,
                                     s2_high_proba_clouds_percentage         double,
                                     s2_thin_cirrus_percentage               double,
                                     s2_snow_ice_percentage                  double,
                                     s2_product_type                         string,
                                     s2_processing_baseline                  string,
                                     s2_product_uri                          string,
                                     s2_generation_time                      string,
                                     s2_datatake_id                          string,
                                     s2_datatake_type                        string,
                                     s2_datastrip_id                         string,
                                     s2_granule_id                           string,
                                     s2_reflectance_conversion_factor        string,
                                     s2_sequence                             string,
                                     earthsearch_s3_path                     string,
                                     earthsearch_payload_id                  string,
                                     earthsearch_boa_offset_applied          bigint,
                                     sentinel2_to_stac                       string,
                                     updated                                 string,
                                     geometry_type                           string,
                                     geometry_coordinates                    string,
                                     bbox                                    string,
                                     features_collection                     string,
                                     thumbnail_acquire_url                   string,
                                     thumbnail_url                           string,
                                     visual_url                              string,
                                     visual_proj_transform                   string,
                                     acquire_data_type                       string, -- 数据的类型
                                     flag                                    string -- 缩略图的下载状态
) with (
      'connector' = 'kafka',
      'topic' = 'sentinel2_list',
      'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      -- 'properties.bootstrap.servers' = '115.231.236.108:30090',
      'properties.group.id' = 'sentinel2_list_group_rt',
      'scan.startup.mode' = 'latest-offset',
      -- 'scan.startup.mode' = 'timestamp',
      -- 'scan.startup.timestamp-millis' = '1730477897000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );



--（Sink：Doris）遥感列表明细
drop table if exists dwd_bhv_sentinel_info;
create table dwd_bhv_sentinel_info (
                                       id                                      string,
                                       photograph_datetime                     string,
                                       resolution                              double,
                                       type                                    string,
                                       stac_version                            string,
                                       context                                 string,
                                       features_type                           string,
                                       created                                 string,
                                       platform                                string,
                                       constellation                           string,
                                       instruments                             string,
                                       instruments_name                        string,
                                       eo_cloud_cover                          double,
                                       proj_epsg                               double,
                                       mgrs_utm_zone                           double,
                                       mgrs_latitude_band                      string,
                                       mgrs_grid_square                        string,
                                       grid_code                               string,
                                       view_sun_azimuth                        double,
                                       view_sun_elevation                      double,
                                       s2_degraded_msi_data_percentage         double,
                                       s2_nodata_pixel_percentage              double,
                                       s2_saturated_defective_pixel_percentage double,
                                       s2_cloud_shadow_percentage              double,
                                       s2_vegetation_percentage                double,
                                       s2_not_vegetated_percentage             double,
                                       s2_water_percentage                     double,
                                       s2_unclassified_percentage              double,
                                       s2_medium_proba_clouds_percentage       double,
                                       s2_high_proba_clouds_percentage         double,
                                       s2_thin_cirrus_percentage               double,
                                       s2_snow_ice_percentage                  double,
                                       s2_product_type                         string,
                                       s2_processing_baseline                  string,
                                       s2_product_uri                          string,
                                       s2_generation_time                      string,
                                       s2_datatake_id                          string,
                                       s2_datatake_type                        string,
                                       s2_datastrip_id                         string,
                                       s2_granule_id                           string,
                                       s2_reflectance_conversion_factor        string,
                                       s2_sequence                             string,
                                       earthsearch_s3_path                     string,
                                       earthsearch_payload_id                  string,
                                       earthsearch_boa_offset_applied          bigint,
                                       sentinel2_to_stac                       string,
                                       updated                                 string,
                                       geometry_type                           string,
                                       geometry_coordinates                    string,
                                       bbox                                    string,
                                       box_lower_left_lng                      double,
                                       box_lower_left_lat					  double,
                                       box_upper_right_lng					  double,
                                       box_upper_right_lat					  double,
                                       features_collection                     string,
                                       thumbnail_acquire_url                   string,
                                       thumbnail_url                           string,
                                       visual_url                              string,
                                       visual_proj_transform                   string,
                                       update_time                             string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dwd_bhv_sentinel_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );




--（Sink：Doris）遥感缩略图的采集情况
drop table if exists dim_entinel2_thumbnail_info;
create table dim_entinel2_thumbnail_info (
                                             id                                      string,
                                             photograph_datetime                     string,
                                             flag                                    string,
                                             update_time                             string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      'table.identifier' = 'sa.dim_entinel2_thumbnail_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- -----------------------

-- 数据处理

-- -----------------------
-- 过滤数据
create view tmp_01 as
select
    *
from sentinel2_list_kafka
where id is not null
  and photograph_datetime is not null;




-- -----------------------

-- 数据写入

-- -----------------------
begin statement set;

insert into  dwd_bhv_sentinel_info
select
    id                                      ,
    photograph_datetime                     ,
    10 as resolution                        ,
    type                                    ,
    stac_version                            ,
    context                                 ,
    features_type                           ,
    created                                 ,
    platform                                ,
    constellation                           ,
    instruments                             ,
    '多光谱成像仪' as instruments,
    eo_cloud_cover                          ,
    proj_epsg                               ,
    mgrs_utm_zone                           ,
    mgrs_latitude_band                      ,
    mgrs_grid_square                        ,
    grid_code                               ,
    view_sun_azimuth                        ,
    view_sun_elevation                      ,
    s2_degraded_msi_data_percentage         ,
    s2_nodata_pixel_percentage              ,
    s2_saturated_defective_pixel_percentage ,
    s2_cloud_shadow_percentage              ,
    s2_vegetation_percentage                ,
    s2_not_vegetated_percentage             ,
    s2_water_percentage                     ,
    s2_unclassified_percentage              ,
    s2_medium_proba_clouds_percentage       ,
    s2_high_proba_clouds_percentage         ,
    s2_thin_cirrus_percentage               ,
    s2_snow_ice_percentage                  ,
    s2_product_type                         ,
    s2_processing_baseline                  ,
    s2_product_uri                          ,
    s2_generation_time                      ,
    s2_datatake_id                          ,
    s2_datatake_type                        ,
    s2_datastrip_id                         ,
    s2_granule_id                           ,
    s2_reflectance_conversion_factor        ,
    s2_sequence                             ,
    earthsearch_s3_path                     ,
    earthsearch_payload_id                  ,
    earthsearch_boa_offset_applied          ,
    sentinel2_to_stac                       ,
    updated                                 ,
    geometry_type                           ,
    geometry_coordinates                    ,
    bbox                                    ,
    cast(JSON_VALUE(bbox,'$.[0]') as double) as box_lower_left_lng,
    cast(JSON_VALUE(bbox,'$.[1]') as double) as box_lower_left_lat,
    cast(JSON_VALUE(bbox,'$.[2]') as double) as box_upper_right_lng,
    cast(JSON_VALUE(bbox,'$.[3]') as double) as box_upper_right_lat,
    features_collection                     ,
    thumbnail_acquire_url                   ,
    concat('/',thumbnail_url) as thumbnail_url,
    visual_url                              ,
    visual_proj_transform                   ,
    from_unixtime(unix_timestamp())    as update_time

from tmp_01
where acquire_data_type = 'atr'
   or acquire_data_type is null;


insert into dim_entinel2_thumbnail_info
select
    id,
    photograph_datetime,
    flag,
    from_unixtime(unix_timestamp())    as update_time
from tmp_01
where acquire_data_type = 'thumbnail';

end;


