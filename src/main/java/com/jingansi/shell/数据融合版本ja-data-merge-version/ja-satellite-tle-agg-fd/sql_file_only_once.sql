
insert into sa.dws_bhv_satellite_list_fd
select
    id,
    to_date('2023-04-01') as today_time,
    satellite_no,
    src_code,
    epoch_time,
    acquire_time,
    satellite_name,
    line1,
    line2,
    one_line_no,
    elset_classification,
    international_designator,
    utc,
    mean_motion_1st_derivative,
    mean_motion_2nd_derivative,
    bstar_drag_term,
    element_set_type,
    element_number,
    checksum1,
    two_line_no,
    satellite_no2,
    orbit_inclination,
    right_ascension_ascending_node,
    eccentricity,
    argument_perigee,
    mean_anomaly,
    mean_motion,
    revolution_epoch_number,
    checksum2,
    object_status_code,
    object_status_name,
    decay_date,
    now() as update_time
from(
        select
            t1.*,
            if(decay_date <= '2023-04-01','D',null) as object_status_code,
            if(decay_date <= '2023-04-01','衰败',null) as object_status_name,
            decay_date,
            row_number() over(partition by t1.id order by epoch_time desc,src_code asc) as rn
        from(
                select
                    satellite_no as id,
                    *
                from sa.dwd_bhv_satellite_rt
                where epoch_time <= '2023-04-01'

                union all

                select
                    *
                    except(today_time,object_status_code,object_status_name,decay_date)
                from sa.dws_bhv_satellite_list_fd
				where today_time = to_date(date_sub('2023-04-01',1))
            ) t1
        left join sa.dws_et_satellite_info as t2
        on t1.id=t2.id
    ) a
where rn = 1;


