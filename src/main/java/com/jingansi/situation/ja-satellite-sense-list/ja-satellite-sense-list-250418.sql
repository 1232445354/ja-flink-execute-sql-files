--********************************************************************--
-- author:      write your name here
-- create time: 2024/10/31 09:51:42
-- description: 卫星遥感数据处理，哨兵2、landsat
-- version: ja-satellite-sense-list-v250409
--********************************************************************--
set 'pipeline.name' = 'ja-satellite-sense-list';

-- set 'parallelism.default' = '2';
-- set 'execution.type' = 'streaming';
-- set 'table.planner' = 'blink';
set 'table.exec.state.ttl' = '300000';
set 'sql-client.execution.result-mode' = 'TABLEAU';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '100000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-satellite-sense-list';


 -- -----------------------

 -- 数据结构

 -- -----------------------

-- 哨兵2数据
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
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '1744984234000',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


-- landsat数据
create table landsat_sense_list_kafka(
                                         id                       string,
                                         photograph_datetime      string,
                                         utc_photograph_datetime  string,
                                         type                     string,
                                         stac_version             string,
                                         context                  string,
                                         features_type            string,
                                         created                  string,
                                         platform                 string,
                                         instruments              string,
                                         eo_cloud_cover           double,
                                         proj_epsg                double,
                                         view_sun_azimuth         double,
                                         view_sun_elevation       double,
                                         updated                  string,
                                         geometry_type            string,
                                         geometry_coordinates     string,
                                         bbox                     string,
                                         visual_proj_transform    string,
                                         specification_version    string,

                                         geometric_rmse           double,
                                         cloud_cover_land         double,
                                         view_off_nadir           double,
                                         geometric_x_bias         double,
                                         geometric_y_bias         double,
                                         geometric_x_stddev       double,
                                         geometric_y_stddev       double,
                                         description              string,
                                         scene_id                 string,
                                         specification            string,
                                         collection_category      string,
                                         wrs_type                 string,
                                         wrs_path                 string,
                                         proj_shape               string,
                                         correction               string,
                                         collection_number        string,
                                         wrs_row                  string,

                                         small_thumbnail_s3_path     string,
                                         small_thumbnail_acquire_url string,
                                         small_thumbnail_url         string,
                                         large_thumbnail_s3_path     string,
                                         large_thumbnail_acquire_url string,
                                         large_thumbnail_url         string,

                                         blue_b2_s3_path             string,
                                         green_b3_s3_path            string,
                                         red_b4_s3_path              string,
                                         blue_b2_acquire_url         string,
                                         green_b3_acquire_url        string,
                                         red_b4_acquire_url          string,

                                         acquire_data_type           string,
                                         small_flag                  string,
                                         large_flag                  string

) with (
      'connector' = 'kafka',
      'topic' = 'landsat_sense_list',
      -- 'properties.bootstrap.servers' = 'kafka-0.kafka-headless.base.svc.cluster.local:9092,kafka-1.kafka-headless.base.svc.cluster.local:9092,kafka-2.kafka-headless.base.svc.cluster.local:9092',
      'properties.bootstrap.servers' = '115.231.236.108:30090',
      'properties.group.id' = 'landsat_sense_list_01',
      -- 'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




--（Sink：Doris）遥感列表明细
create table dwd_bhv_satellite_sense_info (
                                              id                                      string,
                                              photograph_datetime                     string,
                                              src_code                                bigint,
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

                                              blue_b2_s3_path                         string,
                                              green_b3_s3_path                        string,
                                              red_b4_s3_path                          string,
                                              blue_b2_acquire_url                     string,
                                              green_b3_acquire_url                    string,
                                              red_b4_acquire_url                      string,

                                              update_time                             string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      -- 'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dwd_bhv_satellite_sense_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );



--（Sink：Doris）遥感缩略图的采集情况
create table dim_satellite_sense_thumbnail_info (
                                                    id                    string,
                                                    photograph_datetime   string,
                                                    src_code              bigint,
                                                    flag                  string,
                                                    small_flag            string,
                                                    large_flag            string,
                                                    update_time           string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      -- 'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dim_satellite_sense_thumbnail_info',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- landsat单独入库的表
create table dwd_bhv_landsat_sense_list (
                                            id                       string ,
                                            photograph_datetime      string ,
                                            utc_photograph_datetime  string ,
                                            type                     string ,
                                            stac_version             string ,
                                            context                  string ,
                                            features_type            string ,
                                            created                  string ,
                                            platform                 string ,
                                            instruments              string ,
                                            eo_cloud_cover           double ,
                                            proj_epsg                double ,
                                            view_sun_azimuth         double ,
                                            view_sun_elevation       double ,
                                            updated                  string ,
                                            geometry_type            string ,
                                            geometry_coordinates     string ,
                                            bbox                     string ,
                                            visual_proj_transform    string ,
                                            specification_version    string ,

                                            geometric_rmse           double ,
                                            cloud_cover_land         double ,
                                            view_off_nadir           double ,
                                            geometric_x_bias         double ,
                                            geometric_y_bias         double ,
                                            geometric_x_stddev       double ,
                                            geometric_y_stddev       double ,
                                            description              string ,
                                            scene_id                 string ,
                                            specification            string ,
                                            collection_category      string ,
                                            wrs_type                 string ,
                                            wrs_path                 string ,
                                            wrs_row                  string ,
                                            proj_shape               string ,
                                            correction               string ,
                                            collection_number        string ,

                                            small_thumbnail_s3_path     string ,
                                            small_thumbnail_acquire_url string ,
                                            small_thumbnail_url         string ,
                                            large_thumbnail_s3_path     string ,
                                            large_thumbnail_acquire_url string ,
                                            large_thumbnail_url         string ,

                                            blue_b2_s3_path             string ,
                                            green_b3_s3_path            string ,
                                            red_b4_s3_path              string ,
                                            blue_b2_acquire_url         string ,
                                            green_b3_acquire_url        string ,
                                            red_b4_acquire_url          string ,
                                            update_time                 string
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.245:8030',
      -- 'fenodes' = '172.21.30.105:30030',
      'table.identifier' = 'sa.dwd_bhv_landsat_sense_list',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='1',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='10000',
      'sink.batch.interval'='10s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );


-- -----------------------

-- 数据处理

-- -----------------------

-- 哨兵数据处理-过滤数据
create view tmp_01 as
select
    *,
    1               as src_code,
    '多光谱成像仪'   as instruments_name,
    10 as resolution,
    cast(JSON_VALUE(bbox,'$.[0]') as double) as box_lower_left_lng,
    cast(JSON_VALUE(bbox,'$.[1]') as double) as box_lower_left_lat,
    cast(JSON_VALUE(bbox,'$.[2]') as double) as box_upper_right_lng,
    cast(JSON_VALUE(bbox,'$.[3]') as double) as box_upper_right_lat
from sentinel2_list_kafka
where id is not null
  and photograph_datetime is not null;



-- landsat数据处理
create view tmp01_landsat as
select
    *,
    2 as src_code,
    30 as resolution,   -- 分辨率
    instruments as instruments_name, -- 仪器
    'landsat' as constellation,
    cast(JSON_VALUE(bbox,'$.[0]') as double) as box_lower_left_lng,
    cast(JSON_VALUE(bbox,'$.[1]') as double) as box_lower_left_lat,
    cast(JSON_VALUE(bbox,'$.[2]') as double) as box_upper_right_lng,
    cast(JSON_VALUE(bbox,'$.[3]') as double) as box_upper_right_lat
from landsat_sense_list_kafka
where id is not null
  and photograph_datetime is not null;

-- -----------------------

-- 数据写入

-- -----------------------
begin statement set;

-- landsat数据入库
insert into dwd_bhv_satellite_sense_info
select
    id                                      ,
    photograph_datetime                     ,
    src_code                                ,
    resolution                              ,
    type                                    ,
    stac_version                            ,
    context                                 ,
    features_type                           ,
    created                                 ,
    platform                                ,
    constellation  ,
    instruments                             ,
    instruments_name                        ,
    eo_cloud_cover                          ,
    proj_epsg                               ,
    cast(null as double) as mgrs_utm_zone     ,
    cast(null as varchar) as mgrs_latitude_band,
    cast(null as varchar) as mgrs_grid_square  ,
    cast(null as varchar) as grid_code         ,
    view_sun_azimuth                        ,
    view_sun_elevation                      ,
    cast(null as double) as s2_degraded_msi_data_percentage         ,
    cast(null as double) as s2_nodata_pixel_percentage              ,
    cast(null as double) as s2_saturated_defective_pixel_percentage ,
    cast(null as double) as s2_cloud_shadow_percentage              ,
    cast(null as double) as s2_vegetation_percentage                ,
    cast(null as double) as s2_not_vegetated_percentage             ,
    cast(null as double) as s2_water_percentage                     ,
    cast(null as double) as s2_unclassified_percentage              ,
    cast(null as double) as s2_medium_proba_clouds_percentage       ,
    cast(null as double) as s2_high_proba_clouds_percentage         ,
    cast(null as double) as s2_thin_cirrus_percentage               ,
    cast(null as double) as s2_snow_ice_percentage                  ,
    cast(null as varchar) as s2_product_type                         ,
    cast(null as varchar) as s2_processing_baseline                  ,
    cast(null as varchar) as s2_product_uri                          ,
    cast(null as varchar) as s2_generation_time                      ,
    cast(null as varchar) as s2_datatake_id                          ,
    cast(null as varchar) as s2_datatake_type                        ,
    cast(null as varchar) as s2_datastrip_id                         ,
    cast(null as varchar) as s2_granule_id                           ,
    cast(null as varchar) as s2_reflectance_conversion_factor        ,
    cast(null as varchar) as s2_sequence                             ,
    cast(null as varchar) as earthsearch_s3_path                     ,
    cast(null as varchar) as earthsearch_payload_id                  ,
    cast(null as bigint)  as earthsearch_boa_offset_applied          ,
    cast(null as varchar) as sentinel2_to_stac                       ,
    updated                                 ,
    geometry_type                           ,
    geometry_coordinates                    ,
    bbox                                    ,
    box_lower_left_lng,
    box_lower_left_lat,
    box_upper_right_lng,
    box_upper_right_lat,
    cast(null as varchar) as features_collection,
    large_thumbnail_acquire_url  as thumbnail_acquire_url,
    concat('/',large_thumbnail_url) as thumbnail_url,
    cast(null as varchar) as visual_url,
    visual_proj_transform,
    blue_b2_s3_path,
    green_b3_s3_path,
    red_b4_s3_path,
    blue_b2_acquire_url,
    green_b3_acquire_url,
    red_b4_acquire_url,
    from_unixtime(unix_timestamp())    as update_time
from tmp01_landsat
where acquire_data_type = 'atr';



-- landsat数据入库-缩略图
insert into dim_satellite_sense_thumbnail_info
select
    id,
    photograph_datetime,
    src_code,
    cast(null as varchar) as flag,
    small_flag,
    large_flag,
    from_unixtime(unix_timestamp())    as update_time
from tmp01_landsat
where acquire_data_type = 'thumbnail';



-- landsat数据入库-独立表
insert into dwd_bhv_landsat_sense_list
select
    id                       ,
    photograph_datetime      ,
    utc_photograph_datetime  ,
    type                     ,
    stac_version             ,
    context                  ,
    features_type            ,
    created                  ,
    platform                 ,
    instruments              ,
    eo_cloud_cover           ,
    proj_epsg                ,
    view_sun_azimuth         ,
    view_sun_elevation       ,
    updated                  ,
    geometry_type            ,
    geometry_coordinates     ,
    bbox                     ,
    visual_proj_transform    ,
    specification_version    ,

    geometric_rmse           ,
    cloud_cover_land         ,
    view_off_nadir           ,
    geometric_x_bias         ,
    geometric_y_bias         ,
    geometric_x_stddev       ,
    geometric_y_stddev       ,
    description              ,
    scene_id                 ,
    specification            ,
    collection_category      ,
    wrs_type                 ,
    wrs_path                 ,
    wrs_row                  ,
    proj_shape               ,
    correction               ,
    collection_number        ,

    small_thumbnail_s3_path     ,
    small_thumbnail_acquire_url ,
    small_thumbnail_url         ,
    large_thumbnail_s3_path     ,
    large_thumbnail_acquire_url ,
    large_thumbnail_url         ,

    blue_b2_s3_path             ,
    green_b3_s3_path            ,
    red_b4_s3_path              ,
    blue_b2_acquire_url         ,
    green_b3_acquire_url        ,
    red_b4_acquire_url          ,
    from_unixtime(unix_timestamp())  as update_time
from tmp01_landsat
where acquire_data_type = 'atr';



-- 哨兵数据入库
insert into  dwd_bhv_satellite_sense_info
select
    id                                      ,
    photograph_datetime                     ,
    src_code                                ,
    resolution                              ,
    type                                    ,
    stac_version                            ,
    context                                 ,
    features_type                           ,
    created                                 ,
    platform                                ,
    constellation                           ,
    instruments                             ,
    instruments_name                        ,
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
    box_lower_left_lng,
    box_lower_left_lat,
    box_upper_right_lng,
    box_upper_right_lat,
    features_collection                     ,
    thumbnail_acquire_url                   ,
    concat('/',thumbnail_url) as thumbnail_url,
    visual_url                              ,
    visual_proj_transform                   ,
    cast(null as varchar) as blue_b2_s3_path,
    cast(null as varchar) as green_b3_s3_path,
    cast(null as varchar) as red_b4_s3_path,
    cast(null as varchar) as blue_b2_acquire_url,
    cast(null as varchar) as green_b3_acquire_url,
    cast(null as varchar) as red_b4_acquire_url,
    from_unixtime(unix_timestamp())    as update_time
from tmp_01
where acquire_data_type = 'atr';


-- 哨兵数据入库-缩略图
insert into dim_satellite_sense_thumbnail_info
select
    id,
    photograph_datetime,
    src_code,
    flag,
    cast(null as varchar) as small_flag,
    cast(null as varchar) as large_flag,
    from_unixtime(unix_timestamp())    as update_time
from tmp_01
where acquire_data_type = 'thumbnail';

end;




