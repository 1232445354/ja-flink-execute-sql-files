alter table dwd_radar_target_all_rt
    add column upload_mode VARCHAR(200) COMMENT '模式' after time1;

alter table dwd_radar_target_all_rt
    add column target_state bigint COMMENT '目标状态' after upload_mode;


alter table device_media_datasource add column longitude double comment '经度' after url;

alter table device_media_datasource add column latitude double comment '纬度' after longitude;



