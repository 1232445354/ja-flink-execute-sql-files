#!/bin/bash

DIR=$(cd `dirname $0`; pwd)

source ${DIR}/config.sh

sql="
insert into sa.dws_et_aircraft_info(
	flight_id
  ,acquire_time
  ,src_code
  ,icao_code
  ,pk_type
  ,src_pk
  ,registration
  ,flight_type
  ,model
  ,icao_short_type
  ,category_code
  ,category_name
  ,country_code
  ,country_name
  ,airlines_icao
  ,airlines_e_name
  ,airlines_c_name
  ,is_military
  ,test_reg
  ,registered
  ,reg_until
  ,first_flight_date
  ,engines
  ,m_year
  ,manufacturer_icao
  ,manufacturer_name
  ,line_number
  ,operator
  ,operator_c_name
  ,operator_callsign
  ,operator_icao
  ,operator_iata
  ,owner
  ,category_description
  ,faa_pia
  ,faa_ladd
  ,modes
  ,adsb
  ,acars
  ,is_icao
  ,notes
  ,extend_info
  ,friend_foe
  ,search_content
  ,update_time
)

select
	flight_id
  ,acquire_time
  ,src_code
  ,icao_code
  ,pk_type
  ,src_pk
  ,registration
  ,flight_type
  ,model
  ,icao_short_type
  ,category_code
  ,category_name
  ,country_code
  ,country_name
  ,airlines_icao
  ,airlines_e_name
  ,airlines_c_name
  ,is_military
  ,test_reg
  ,registered
  ,reg_until
  ,first_flight_date
  ,engines
  ,m_year
  ,manufacturer_icao
  ,manufacturer_name
  ,line_number
  ,operator
  ,operator_c_name
  ,operator_callsign
  ,operator_icao
  ,operator_iata
  ,owner
  ,category_description
  ,faa_pia
  ,faa_ladd
  ,modes
  ,adsb
  ,acars
  ,is_icao
  ,notes
  ,extend_info
  ,friend_foe
  ,search_content
  ,update_time
from doris_idc.sa.dws_et_aircraft_info

where acquire_time > to_date(date_sub(now(),interval 1 day));

"

mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"