--********************************************************************--
-- author:      write your name here
-- create time: 2024/11/28 20:34:11
-- description: 埋点日志采集
-- version: ja-et-log-rt-v241202
--********************************************************************--
set 'pipeline.name' = 'ja-et-log-rt';


set 'table.exec.state.ttl' = '500000';
set 'parallelism.default' = '1';

-- checkpoint的时间和位置
set 'execution.checkpointing.interval' = '600000';
set 'execution.checkpointing.timeout' = '3600000';
set 'state.checkpoints.dir' = 's3://ja-flink/flink-checkpoints/ja-et-log-rt';


create table clklog(

                       gzip                 string,
                       type                 string,
                       _track_id            bigint,
                       lib row<
                           $lib                 string,
                       $lib_version         string,
                       $app_version         string,
                       $lib_method          string
                           >,
                       currentTimeMillis    bigint,
                       event                string,
                       project              string,
                       _flush_time          bigint,
                       token                string,
                       distinct_id          string,
                       clientIp             string,
                       `time`               bigint,
                       crc                  string,
                       identities row<
                           user_key             string,
                       $identity_cookie_id  string,
                       $identity_login_id   string,
                       $identity_anonymous_id string
                           >,
                       login_id             string,
                       anonymous_id         string,
                       properties row<
                           $latest_referrer     string,
                       $timezone_offset     int,
                       $lib                 string,
                       $url_path            string,
                       event_duration       double,
                       $lib_version         string,
                       $screen_width        int,
                       $title               string,
                       $url                 string,
                       $user_agent          string,
                       $viewport_height     int,
                       $is_first_day        boolean,
                       $is_first_time       boolean,
                       $viewport_width      int,
                       $page_x              int,
                       $page_y              int,
                       $latest_search_keyword string,
                       $latest_traffic_source_type string,
                       $element_id          string,
                       country              string,
                       province             string,
                       city                 string,
                       $country             string,
                       $province            string,
                       $city                string,
                       $brand               string,
                       $browser             string,
                       $browser_version     string,
                       $carrier             string,
                       $device_id           string,
                       $element_class_name  string,
                       $element_content      string,
                       $element_name         string,
                       $element_position     string,
                       $element_selector     string,
                       $element_target_url   string,
                       $element_type         string,
                       $first_channel_ad_id        string,
                       $first_channel_adgroup_id   string,
                       $first_channel_campaign_id  string,
                       $first_channel_click_id     string,
                       $first_channel_name         string,
                       $latest_landing_page        string,
                       $latest_referrer_host       string,
                       $latest_scene               string,
                       $latest_share_method        string,
                       $latest_utm_campaign        string,
                       $latest_utm_content         string,
                       $latest_utm_medium          string,
                       $latest_utm_source          string,
                       $latest_utm_term            string,
                       $latitude                   double,
                       $longitude                  double,
                       $manufacturer               string,
                       $matched_key                string,
                       $matching_key_list          string,
                       $model                      string,
                       $network_type               string,
                       $receive_time               bigint,
                       $screen_name                string,
                       $screen_orientation         string,
                       $short_url_key              string,
                       $short_url_target           string,
                       $source_package_name        string,
                       $track_signup_original_id   string,
                       $utm_campaign               string,
                       $utm_content                string,
                       $utm_matching_type          string,
                       $utm_medium                 string,
                       $utm_source                 string,
                       $utm_term                   string,
                       $referrer                   string,
                       $referrer_host              string,
                       $wifi                       string,
                       DownloadChannel             string,
                       $event_session_id           string,
                       $app_id                     string,
                       $app_name                   string,
                       $app_state                  string,
                       $os                         string,
                       $os_version                 string,
                       $event_duration             double,
                       $screen_height       int,
                       $viewport_position   int
                           >,
                       `kafka_data_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) with (
      'connector' = 'kafka',
      'topic' = 'clklog',
      'properties.bootstrap.servers' = 'kafka.base.svc.cluster.local:9092',
      --'properties.bootstrap.servers' = ':30090',
      'properties.group.id' = 'ja-et-log-rt',
      'scan.startup.mode' = 'timestamp',
      'scan.startup.timestamp-millis' = '0',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );




create table dwd_log_analysis_rt (
                                     distinct_id                    string        comment '',
                                     type_context                   string        comment '',
                                     event                          string        comment '',
                                     log_time                       timestamp     comment '',
                                     stat_date                      string        comment '',
                                     stat_hour                      int           comment '',
                                     flush_time                     timestamp     comment '',
                                     track_id                       bigint        comment '',
                                     identity_cookie_id             string        comment '',
                                     identity_login_id 			   String        comment '',
                                     identity_anonymous_id 		   String        comment '',
                                     login_id 					   String        comment '',
                                     anonymous_id 				   String        comment '',
                                     lib                            string        comment '',
                                     lib_method                     string        comment '',
                                     lib_version                    string        comment '',
                                     gzip 						   String        comment '',
                                     timezone_offset                int           comment '',
                                     screen_height                  int           comment '',
                                     screen_width                   int           comment '',
                                     page_x 					       int           comment '',
                                     page_y 					       int           comment '',
                                     viewport_height                int           comment '',
                                     viewport_width                 int           comment '',
                                     referrer                       string        comment '',
                                     url                            string        comment '',
                                     url_path                       string        comment '',
                                     title                          string        comment '',
                                     latest_referrer                string        comment '',
                                     latest_search_keyword          string        comment '',
                                     latest_traffic_source_type     string        comment '',
                                     is_first_day                   boolean       comment '',
                                     is_first_time                  boolean        comment '',
                                     referrer_host                  string        comment '',
                                     element_id                     string        comment '',
                                     country                        string        comment '',
                                     province                       string        comment '',
                                     city                           string        comment '',
                                     app_id                         string        comment '',
                                     app_name                       string        comment '',
                                     app_state                      string        comment '',
                                     app_version                    string        comment '',
                                     brand                          string        comment '',
                                     browser                        string        comment '',
                                     browser_version                string        comment '',
                                     carrier                        string        comment '',
                                     device_id                      string        comment '',
                                     element_class_name             string        comment '',
                                     element_content                string        comment '',
                                     element_name                   string        comment '',
                                     element_position               string        comment '',
                                     element_selector               string        comment '',
                                     element_target_url             string        comment '',
                                     element_type                   string        comment '',
                                     first_channel_ad_id            string        comment '',
                                     first_channel_adgroup_id       string        comment '',
                                     first_channel_campaign_id      string        comment '',
                                     first_channel_click_id         string        comment '',
                                     first_channel_name             string        comment '',
                                     latest_landing_page            string        comment '',
                                     latest_referrer_host           string        comment '',
                                     latest_scene                   string        comment '',
                                     latest_share_method            string        comment '',
                                     latest_utm_campaign            string        comment '',
                                     latest_utm_content             string        comment '',
                                     latest_utm_medium              string        comment '',
                                     latest_utm_source              string        comment '',
                                     latest_utm_term                string        comment '',
                                     latitude                       double        comment '',
                                     longitude                      double        comment '',
                                     manufacturer                   string        comment '',
                                     matched_key                    string        comment '',
                                     matching_key_list              string        comment '',
                                     model                          string        comment '',
                                     network_type                   string        comment '',
                                     os                             string        comment '',
                                     os_version                     string        comment '',
                                     receive_time                   timestamp     comment '',
                                     screen_name                    string        comment '',
                                     screen_orientation             string        comment '',
                                     short_url_key                  string        comment '',
                                     short_url_target               string        comment '',
                                     source_package_name            string        comment '',
                                     track_signup_original_id       string        comment '',
                                     user_agent                     string        comment '',
                                     utm_campaign                   string        comment '',
                                     utm_content                    string        comment '',
                                     utm_matching_type              string        comment '',
                                     utm_medium                     string        comment '',
                                     utm_source                     string        comment '',
                                     utm_term                       string        comment '',
                                     viewport_position              bigint        comment '',
                                     wifi                           string        comment '',
                                     event_duration                 double        comment '',
                                     download_channel               string        comment '',
                                     user_key                       string        comment '',
                                     is_logined                     int           comment '',
                                     event_session_id               string        comment '',
                                     raw_url                        string        comment '',
                                     crc                            string        comment '',
                                     is_compress                    string        comment '',
                                     client_ip                      string        comment '',
                                     kafka_data_time                timestamp        comment '',
                                     project_name                   string        comment '',
                                     project_token                  string        comment '',
                                     create_time                    timestamp     comment ''
) with (
      'connector' = 'doris',
      'fenodes' = '172.21.30.246:8030',
      'table.identifier' = 'et_log.dwd_log_analysis_rt',
      'username' = 'admin',
      'password' = 'Jingansi@110',
      'doris.request.tablet.size'='5',
      'doris.request.read.timeout.ms'='30000',
      'sink.batch.size'='30000',
      'sink.batch.interval'='15s',
      'sink.properties.escape_delimiters' = 'true',
      'sink.properties.column_separator' = '\x01',	 -- 列分隔符
      'sink.properties.escape_delimiters' = 'true',    -- 类似开启的意思
      'sink.properties.line_delimiter' = '\x02'		 -- 行分隔符
      );

-- select * from clklog;



insert into dwd_log_analysis_rt
select
    distinct_id as distinct_id                    , -- string
    `type` as type_context                   , -- string
    event as event                          , -- string
    TO_TIMESTAMP_LTZ(`time`,3) as log_time                       , -- timestamp
    date_format(TO_TIMESTAMP_LTZ(`time`,3),'yyyy-MM-dd') as stat_date                      , -- timestamp
    cast(hour(TO_TIMESTAMP_LTZ(`time`,3)) as int) as stat_hour                      , -- int
    TO_TIMESTAMP_LTZ(`_flush_time`,3) as _flush_time                  , -- timestamp
    _track_id as track_id                       , -- bigint
    identities.`$identity_cookie_id` as identity_cookie_id             , -- string
    identities.`$identity_login_id` as identity_login_id 			   , -- String
	identities.`$identity_anonymous_id` as identity_anonymous_id 		   , -- String
	login_id as login_id 					   , -- String
	anonymous_id as anonymous_id 				   , -- String
    lib.`$lib` as lib                            , -- string
    lib.`$lib_method` as lib_method                     , -- string
    lib.`$lib_version` as lib_version                    , -- string
    gzip as gzip 						   , -- String
    properties.`$timezone_offset` as timezone_offset                , -- bigint
    properties.`$screen_height` as screen_height                  , -- int
    properties.`$screen_width` as screen_width                   , -- int
    properties.`$page_x` as page_x 					       , -- int
	properties.`$page_y` as page_y 					       , -- int
    properties.`$viewport_height` as viewport_height                , -- int
    properties.`$viewport_width` as viewport_width                 , -- int
    properties.`$referrer` as referrer                       , -- string
    properties.`$url` as url                            , -- string
    nullif(REGEXP_REPLACE(REGEXP_EXTRACT( properties.`$url`, '(?<=#)([^#?]*)(?=\?|$)', 0), '/$', ''),'') as url_path                       , -- string
    properties.`$title` as title                          , -- string
    properties.`$latest_referrer` as latest_referrer                , -- string
    properties.`$latest_search_keyword` as latest_search_keyword          , -- string
    properties.`$latest_traffic_source_type` as latest_traffic_source_type     , -- string
    properties.`$is_first_day` as is_first_day                   , -- boolean
    properties.`$is_first_time` as is_first_time                  , -- boolean
    properties.`$referrer_host` as referrer_host                  , -- string
    properties.`$element_id` as element_id                     , -- string
    coalesce(properties.`$country`,properties.country) as country                        , -- string
    coalesce(properties.`$province`,properties.province) as province                       , -- string
    coalesce(properties.`$city`,properties.city) as city                           , -- string
    properties.`$app_id` as app_id                         , -- string
    properties.`$app_name` as app_name                       , -- string
    properties.`$app_state` as app_state                      , -- string
    lib.`$app_version` as app_version                    , -- string
    properties.`$brand` as brand                          , -- string
    properties.`$browser` as browser                        , -- string
    properties.`$browser_version` as browser_version                , -- string
    properties.`$carrier`as carrier                        , -- string
    properties.`$device_id` as device_id                      , -- string
    properties.`$element_class_name` as element_class_name             , -- string
    properties.`$element_content`as element_content                , -- string
    properties.`$element_name` as element_name                   , -- string
    properties.`$element_position` as element_position               , -- string
    properties.`$element_selector` as element_selector               , -- string
    properties.`$element_target_url` as element_target_url             , -- string
    properties.`$element_type` as element_type                   , -- string
    properties.`$first_channel_ad_id` as first_channel_ad_id            , -- string
    properties.`$first_channel_adgroup_id` as first_channel_adgroup_id       , -- string
    properties.`$first_channel_campaign_id` as first_channel_campaign_id      , -- string
    properties.`$first_channel_click_id` as first_channel_click_id         , -- string
    properties.`$first_channel_name` as first_channel_name             , -- string
    properties.`$latest_landing_page` as latest_landing_page            , -- string
    properties.`$latest_referrer_host` as latest_referrer_host           , -- string
    properties.`$latest_scene` as latest_scene                   , -- string
    properties.`$latest_share_method` as latest_share_method            , -- string
    properties.`$latest_utm_campaign` as latest_utm_campaign            , -- string
    properties.`$latest_utm_content` as latest_utm_content             , -- string
    properties.`$latest_utm_medium` as latest_utm_medium              , -- string
    properties.`$latest_utm_source` as latest_utm_source              , -- string
    properties.`$latest_utm_term`as latest_utm_term                , -- string
    properties.`$latitude` as latitude                       , -- double
    properties.`$longitude` as longitude                      , -- double
    properties.`$manufacturer` as manufacturer                   , -- string
    properties.`$matched_key` as matched_key                    , -- string
    properties.`$matching_key_list` as matching_key_list              , -- string
    properties.`$model` as model                          , -- string
    properties.`$network_type` as network_type                   , -- string
    properties.`$os` as os                             , -- string
    properties.`$os_version` as os_version                     , -- string
    TO_TIMESTAMP_LTZ(properties.`$receive_time`,3) as receive_time                   , -- string
    properties.`$screen_name` as screen_name                    , -- string
    properties.`$screen_orientation` as screen_orientation             , -- string
    properties.`$short_url_key` as short_url_key                  , -- string
    properties.`$short_url_target` as short_url_target               , -- string
    properties.`$source_package_name` as source_package_name            , -- string
    properties.`$track_signup_original_id` as track_signup_original_id       , -- string
    properties.`$user_agent` as user_agent                     , -- string
    properties.`$utm_campaign` as utm_campaign                   , -- string
    properties.`$utm_content` as utm_content                    , -- string
    properties.`$utm_matching_type` as utm_matching_type              , -- string
    properties.`$utm_medium` as utm_medium                     , -- string
    properties.`$utm_source` as utm_source                     , -- string
    properties.`$utm_term` as utm_term                       , -- string
    properties.`$viewport_position` as viewport_position              , -- bigint
    properties.`$wifi` as wifi                           , -- string
    properties.`$event_duration` as event_duration                 , -- double
    properties.DownloadChannel as download_channel               , -- string
    identities.user_key as user_key                       , -- string
    if(identities.user_key is not null ,1,cast(null as int)) as is_logined                     , -- int
    properties.`$event_session_id` as event_session_id         , -- string
    properties.`$url` as raw_url                        , -- string
    crc as crc                            , -- string
    cast(null as string) as is_compress                    , -- string
    clientIp as client_ip                      , -- string
    kafka_data_time as kafka_data_time                , -- string
    project as project_name                   , -- string
    token as project_token                  , -- string
    now() as create_time                      -- timestamp
from clklog;
