#!/bin/bash

ip=${1:-server-1}
port=${2:-31030}
pwd=${3:-Jingansi@110}

rm /data1/basedata/mysql/database_structure.sql > /dev/null 2>&1

cp /data1/k8s-package-data/database_structure.sql /data1/basedata/mysql/

kubectl exec -it mysql-0 -nbase -- sh -c " mysql -h$ip -P$port -p$pwd -uroot -e 'source /var/lib/mysql/database_structure.sql;' "
kubectl exec -it mysql-0 -nbase -- sh -c " mysql -h$ip -P$port -p$pwd -uroot -e 'SET global time_zone = \"Asia/Shanghai\";' "
rm /data1/basedata/mysql/database_structure.sql
