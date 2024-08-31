insert into sa.dws_et_aircraft_info
select
    t1.flight_id
     ,t1.acquire_time
     ,t1.src_code
     ,t1.icao_code
     ,t1.pk_type
     ,t1.src_pk
     ,t1.registration
     ,t1.flight_type
     ,t1.model
     ,t1.icao_short_type
     ,t1.category_code
     ,t1.category_name
     ,t1.country_code1 as country_code
     ,t2.c_name as country_name
     ,t1.airlines_icao
     ,t1.airlines_e_name
     ,t1.airlines_c_name
     ,t1.is_military
     ,t1.test_reg
     ,t1.registered
     ,t1.reg_until
     ,t1.first_flight_date
     ,t1.engines
     ,t1.m_year
     ,t1.manufacturer_icao
     ,t1.manufacturer_name
     ,t1.line_number
     ,t1.operator
     ,t1.operator_c_name
     ,t1.operator_callsign
     ,t1.operator_icao
     ,t1.operator_iata
     ,t1.owner
     ,t1.category_description
     ,t1.faa_pia
     ,t1.faa_ladd
     ,t1.modes
     ,t1.adsb
     ,t1.acars
     ,t1.is_icao
     ,t1.notes
     ,null as extend_info
     ,case when country_code1 in('AU','TW','JP','IN','US') and is_military = 1 then 'ENEMY'
           when country_code1 in('CN','HK','MO')  and is_military = 1 then 'OUR_SIDE'
           when country_code1 in('CN','HK','MO')  and is_military = 0 then 'FRIENDLY_SIDE'
           else 'NEUTRALITY'
    end as friend_foe

     ,concat(
        ifnull(icao_code,''),' ',
        ifnull(registration,''),' ',
        ifnull(flight_type,''),' '
    ) as search_content

     ,null as check_cols
     ,null as check_update_time
     ,now() as update_time
from (
         select
             *,
             case when country_code = 'CN' and hou1 = 'H' and hou_len = 3 then 'HK' -- '中国香港'
                  when country_code = 'CN' and hou1 = 'M' and hou_len = 3 then 'MO'  -- 澳门
                  when country_code = 'CN' and hou_len = 5 then 'TW' -- '中国台湾'
                  else country_code
                 end as country_code1

         from (
                  select
                      *,
                      substring(replace(registration,'-',''),1,1) as qian1,
                      substring(replace(registration,'-',''),2,20) as hou,
                      substring(replace(registration,'-',''),2,1) as hou1,
                      length(substring(replace(registration,'-',''),2,20)) as hou_len
                  from (

                           select
                               coalesce(t2.icao_code,t1.flight_id) as flight_id,
                               coalesce(t1.acquire_time,now()) as acquire_time,
                               if(t2.icao_code is not null,'origin',t1.src_code) as src_code,
                               coalesce(t2.icao_code,t1.icao_code) as icao_code,
                               if(t2.icao_code is not null,'origin',t1.pk_type) as pk_type,
                               coalesce(t2.icao_code,t1.src_pk) as src_pk,
                               coalesce(t2.registration,t1.registration) as registration,
                               coalesce(t2.icao_type,t1.flight_type) as flight_type,
                               t2.model as model,
                               t2.icao_short_type,
                               coalesce(t2.category_code,t1.category_code) as category_code,
                               coalesce(t2.category_c_name,t1.category_name) as category_name,
                               coalesce(t2.country_code,t1.country_code) as country_code,
                               t1.airlines_icao as airlines_icao,
                               t1.airlines_e_name as airlines_e_name,
                               t1.airlines_c_name as airlines_c_name,
                               t2.is_mil as is_military,
                               t2.test_reg,
                               t2.registered,
                               t2.reg_until,
                               t2.first_flight_date,
                               t2.engines,
                               t2.m_year,
                               t2.manufacturer_icao,
                               t2.manufacturer_name,
                               t2.line_number,
                               t2.operator,
                               t2.operator_c_name,
                               t2.operator_callsign,
                               t2.operator_icao,
                               t2.operator_iata,
                               t2.owner,
                               t2.category_description,
                               t2.faa_pia,
                               t2.faa_ladd,
                               t2.modes,
                               t2.adsb,
                               t2.acars,
                               coalesce(t2.is_icao,if(t1.icao_code is not null,1,0)) as is_icao,
                               t2.notes
                           from (
                                    select
                                        flight_id,
                                        max_by(src_code,acquire_time) as src_code,
                                        max(acquire_time) as acquire_time,
                                        max(icao_code) as icao_code,
                                        max(registration) as registration,
                                        max(flight_type) as flight_type,
                                        max(flight_category)  as category_code,
                                        max(flight_category_name) as category_name,
                                        max(country_code) as country_code,
                                        max(country_name) as country_name,
                                        max(airlines_icao) as airlines_icao,
                                        max(airlines_e_name) as airlines_e_name,
                                        max(airlines_c_name) as airlines_c_name,
                                        max_by(pk_type,acquire_time) as pk_type,
                                        max_by(src_pk,acquire_time) as src_pk
                                    from sa.dws_aircraft_combine_status_rt
                                    group by flight_id
                                ) as t1
                                    full join (select * from dws_aircraft_info where icao_code is not null and icao_code <> '') as t2
                                              on t1.flight_id = t2.icao_code
                       ) as ttt
              ) as tttt
     ) as t1
         left join sa.dim_country_code_name_info as t2
                   on t1.country_code1 = t2.country_code2