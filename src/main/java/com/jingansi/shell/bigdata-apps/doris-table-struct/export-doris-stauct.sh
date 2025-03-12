#!/bin/bash

ip=server-1
port=31030
pwd=Jingansi@110
dbs="dushu"

kubectl exec -it mysql-0 -nbase -- sh -c " mysqldump -h$ip -P$port -p$pwd -uroot --no-data --no-tablespaces --databases $dbs > /var/lib/mysql/database_structure.sql "

mv /data1/basedata/mysql/database_structure.sql /data1/bigdata/apps/