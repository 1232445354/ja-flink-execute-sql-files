
select
    t1.vessel_id,
    now() as acquire_time,
    case when t2.id is not null and t3.id is not null
             then concat(substr(t2.image_url,1,length(t2.image_url)-1),',',substr(t3.image_url,2,length(t3.image_url)-1))

         when t2.id is not null 	then t2.image_url

         when t3.id is not null  then t3.image_url
         else null end as image_url,

    case when t2.id is not null and t3.id is not null
             then 'marinetraffic+vessel_finder'

         when t2.id is not null 	then 'marinetraffic'

         when t3.id is not null  then 'vessel_finder'
         else null end as source,

    null as remark,
    null as creat_by,
    now() as update_time

from dws_ais_vessel_detail_static_attribute as t1 	-- dws_et_vessel_info
         left join ads_vessel_image_info as t2
                   on t1.vessel_id = t2.id
         left join dws_vessel_image_info2 as t3
                   on t1.mmsi = t3.id
where t2.id is not null
   or t3.id is not null
    limit 10000
