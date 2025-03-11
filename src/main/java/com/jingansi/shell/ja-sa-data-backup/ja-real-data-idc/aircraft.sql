insert into dwd_bhv_aircraft_combine_rt(
                                        flight_id
                                       ,acquire_time
                                       ,src_code
                                       ,icao_code
                                       ,registration
                                       ,flight_no
                                       ,callsign
                                       ,flight_type
                                       ,is_military
                                       ,pk_type
                                       ,src_pk
                                       ,longitude
                                       ,latitude
                                       ,speed
                                       ,speed_km
                                       ,altitude_baro
                                       ,altitude_baro_m
                                       ,heading
                                       ,squawk_code
                                       ,flight_status
                                       ,country_code
                                       ,country_name
                                       ,position_country_code2
                                       ,friend_foe
                                       ,filter_col
                                       ,update_time
)
select
    flight_id
     ,acquire_time
     ,src_code
     ,icao_code
     ,registration
     ,flight_no
     ,callsign
     ,flight_type
     ,is_military
     ,pk_type
     ,src_pk
     ,longitude
     ,latitude
     ,speed
     ,speed_km
     ,altitude_baro
     ,altitude_baro_m
     ,heading
     ,squawk_code
     ,flight_status
     ,country_code
     ,country_name
     ,position_country_code2
     ,friend_foe
     ,filter_col
     ,update_time

from doris_idc.sa.dwd_bhv_aircraft_combine_rt
where acquire_time between '2025-03-04 03:00:00' and '2025-03-04 03:10:00'


