#!/bin/bash

start_time=$(date -d "yesterday" "+%Y-%m-%d")
end_time=$(date "+%Y-%m-%d")
front_day=1
after_day=1
front_operator="-"
after_operator="-"
pre_start_time=$(date -d "${start_time} ${front_operator}${front_day} day" "+%Y-%m-%d 00:00:00")
next_end_time=$(date -d "${end_time} ${after_operator}${after_day} day" "+%Y-%m-%d 00:00:00")


echo -en "start_time::${start_time}\n"
echo -en "end_time::${end_time}\n"
echo -en "pre_start_time::${pre_start_time}\n"
echo -en "next_end_time::${next_end_time}\n"

