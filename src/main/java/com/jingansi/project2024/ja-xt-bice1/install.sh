#!/bin/bash
echo -en "开始启动flink程序...$(date)\n"
sh /data1/bigdata/apps/ship-marinetraffic-real-data/start.sh
sleep 2s
sh /data1/bigdata/apps/ship-vx-real-data/start.sh
sleep 2s
sh /data1/bigdata/apps/fly-radarbox-real-data/start.sh
sleep 2s
sh /data1/bigdata/apps/fly-adsbexchange-real-data/start.sh
sleep 2s
sh /data1/bigdata/apps/fly-flightrader24-real-data/start.sh

#sleep 2s
#sh /data1/bigdata/apps/ship-lb-real-data/start.sh

echo -en "启动完成...$(date)\n"
echo "--------------------------------\n"