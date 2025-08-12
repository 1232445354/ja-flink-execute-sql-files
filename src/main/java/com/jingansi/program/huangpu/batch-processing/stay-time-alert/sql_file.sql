
insert into ja_patrol_control.ads_security_person_stay_time_analysis_ds
select
    security_person_no, --  安保人员编号
    data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    min(stay_start_time) as stay_start_time , -- 停留开始时间
    max(stay_end_time) as stay_end_time , -- 停留结束时间
    security_person_name, -- 安保人员姓名
    security_person_card_id, -- 安保人员证件号
    security_person_phone_no , -- 安保人员手机号
    security_person_photo_url , -- 安保人员头像url
    security_person_type , -- 安保人员类型
    device_id, -- 设备编号（执法仪的编号和微信的设备号）
    device_name, -- 设备名称
    device_alias_name, -- 设备俗称
    walkie_talkie_no, -- 手台号
    organization_id , -- 机构编码
    organization_name , -- 机构名称
    avg(gd_longitude) as gdLongitude , -- 高德经度
    avg(gd_latitude) as gdLatitude , -- 高德纬度
    gd_geohash8,
    unix_timestamp(max(stay_end_time)) - unix_timestamp(min(stay_start_time)) as stay_time , -- 停留时间
    min(window_start) as window_start , -- 窗口开始时间
    max(window_end) as window_end , -- 窗口结束时间
    max(insert_time) as insert_time, -- 插入时间
    max(is_processed) as is_processed , -- 是否已处理 0 未处理 1 已处理
    max(processed_time) as processed_time  -- 处理时间

from ja_patrol_control.ads_security_person_stay_time_alarm_rt
where stay_start_time between DATE_FORMAT(date_sub(now(),interval 1 day),'%Y-%m-%d 00:00:00') and curdate()
group by
    security_person_no , --  安保人员编号
    data_source, -- 数据来源（执法记录仪：LawEnforcement 警务微信：WeChat）
    security_person_name , -- 安保人员姓名
    security_person_card_id, -- 安保人员证件号
    security_person_phone_no, -- 安保人员手机号
    security_person_photo_url, -- 安保人员头像url
    security_person_type, -- 安保人员类型
    device_id , -- 设备编号（执法仪的编号和微信的设备号）
    device_name, -- 设备名称
    device_alias_name, -- 设备俗称
    walkie_talkie_no, -- 手台号
    organization_id, -- 机构编码
    organization_name, -- 机构名称
    gd_geohash8
