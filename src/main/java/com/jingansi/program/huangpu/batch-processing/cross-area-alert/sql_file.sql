
insert into ja_patrol_control.ads_security_person_cross_region_patrol_analysis_ds
select
    security_person_no,
    date_format(min(acquisition_time),'%Y-%m-%d %H:%i:%s') as cross_region_start_time,
    security_person_name as security_person_name,
    walkie_talkie_no as walkie_talkie_no,
    organization_id as organization_id,
    organization_name as organization_name,
    segment,
    max(distance) as cross_region_distance,
    unix_timestamp(max(acquisition_time))- unix_timestamp(min(acquisition_time)) as cross_region_time,
    date_format(max(acquisition_time),'%Y-%m-%d %H:%i:%s') as alarm_time
from(

        select
            *,
            sum(if(unix_timestamp(acquisition_time)-unix_timestamp(pre_acquisition_time) < 600 or pre_acquisition_time is null,0,1))
                over(partition by t1.security_person_no order by acquisition_time asc) as segment
        from (
                 select
                     a.security_person_no as security_person_no,
                     security_person_name,
                     acquisition_time,
                     walkie_talkie_no,
                     c.area_points,
                     a.organization_id as organization_id,
                     a.organization_name as organization_name,
                     lag(acquisition_time,1,null) over(partition by a.security_person_no order by acquisition_time asc) as pre_acquisition_time,
                             ja_patrol_control.getbearing(gd_longitude,gd_latitude,area_points) * 1000 as distance
                 from (
                          select
                              security_person_no,
                              security_person_name,
                              acquisition_time,
                              walkie_talkie_no,
                              organization_id,
                              organization_name,
                              row_number() over(partition by security_person_no,gd_longitude,gd_latitude,date_format(acquisition_time,'%Y-%m-%d %H:%i') order by acquisition_time desc) as rn,
                                  gd_longitude,
                              gd_latitude
                          from ja_patrol_control.dws_security_person_trajectory_rt
                          where acquisition_time between to_date(date_sub(now(),interval 1 day)) and to_date(now())
                      ) as a
                          left join ja_patrol_control.dim_organization_info as c
                                    on  a.organization_id = c.organization_id
                 where st_contains(st_polygon( concat('POLYGON((',replace(replace(c.area_points,',',' '),';',','),'))')),ST_Point(gd_longitude,gd_latitude)) = false
                   and a.rn=1
             ) as t1
    ) t2
group by
    security_person_no,
    security_person_name,
    walkie_talkie_no,
    organization_id,
    organization_name,
    segment
having max(distance) >= 100 and unix_timestamp(max(acquisition_time))- unix_timestamp(min(acquisition_time)) > 600
