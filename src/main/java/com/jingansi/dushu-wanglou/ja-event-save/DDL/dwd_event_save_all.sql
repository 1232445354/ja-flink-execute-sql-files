create table dwd_event_save_all (
                                    device_id                 varchar(200)  comment '设备id',
                                    target_id                 varchar(200)  comment '目标id',
                                    parent_id                 varchar(300)  comment '父设备的id',
                                    acquire_timestamp_format  datetime      comment '上游程序上报时间戳-时间戳格式化',
                                    acquire_timestamp         bigint        comment '时间ms',
                                    local_lot_no              varchar(300)  comment '本地批号,设备sn号',
                                    rcs                       bigint        comment 'RCS值',
                                    data_cycle                bigint        comment '数据周期',
                                    recognition_rate          bigint        comment '目标识别概率',
                                    superior_lot_no           bigint        comment '上级批号,每个车辆唯一',
                                    target_type               bigint        comment '目标类型',
                                    target_property           bigint        comment '目标属性',
                                    track_status              bigint        comment '航迹状态',
                                    target_num                bigint        comment '目标数量',
                                    target_model              bigint        comment '目标型号',

                                    x_speed                 bigint          comment 'X向速度',
                                    y_speed                 bigint          comment 'Y向速度',
                                    z_speed                 bigint          comment 'Z向速度',
                                    x_location              bigint          comment '目标位置X',
                                    y_location              bigint          comment '目标位置Y',
                                    z_location              bigint          comment '目标位置Z',
                                    pitch_system_error      bigint          comment '俯仰系统误差',
                                    pitch_random_error      bigint          comment '俯仰随机误差',
                                    slant_range_system_error bigint          comment '斜距随机误差',
                                    slant_range_random_error bigint          comment '斜距系统误差',
                                    speed_random_error      bigint          comment '速度随机误差',
                                    speed_system_error      bigint          comment '速度系统误差',
                                    yaw_system_error        bigint          comment '方位系统误差',
                                    yaw_random_error        bigint          comment '方位随机误差',
                                    overall_speed           bigint          comment '合速度',
                                    source_type             varchar(300)    comment '数据设备来源名称',
                                    source_type_name        varchar(300)    comment '数据设备来源名称,就是设备类型,使用product_key区分的',
                                    device_name             varchar(300)    comment '设备名称',
                                    device_info             varchar(10000)  comment '数据检测的来源[{deviceName,targetId,type}]',
                                    object_label            varchar(300)    comment '目标类型名称-中文',
                                    update_time DATETIME NULL COMMENT '数据入库时间'
) ENGINE=OLAP
UNIQUE KEY(device_id, target_id, parent_id,acquire_timestamp_format)
COMMENT '设备检测数据全部入库'
PARTITION BY RANGE(acquire_timestamp_format)()
DISTRIBUTED BY HASH(device_id) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Etc/UTC",
"dynamic_partition.start" = "-1000",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 3",
"dynamic_partition.buckets" = "2",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "10",
"dynamic_partition.hot_partition_num" = "0"
);