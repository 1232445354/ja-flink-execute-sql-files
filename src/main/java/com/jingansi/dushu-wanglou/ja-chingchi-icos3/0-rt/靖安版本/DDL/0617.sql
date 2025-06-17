alter table device_media_datasource add column action_id bigint comment '行动ID' after type

alter table device_media_datasource add column action_item_id bigint comment '子行动ID' after action_id
