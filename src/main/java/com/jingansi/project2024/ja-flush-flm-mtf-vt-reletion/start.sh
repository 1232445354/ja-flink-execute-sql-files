#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
source ${DIR}/config.sh
echo -en "开始刷新fleetmon、marinetraffic、vt对应关系数据...$(date)\n"

sh ${DIR}/sql_file.sh

echo -en "--数据生成SUCCESS--------$(date)\n"
echo -en "----------------------------------\n"
