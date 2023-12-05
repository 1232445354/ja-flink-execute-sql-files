#!/bin/bash

echo -en "开始备份数据...$(date)"
mysql -h172.21.30.202 -P31030 -uadmin -pJingansi@110  sa < /data1/bigdata/apps/ja-sa-data-backup/ja-static-data/sql_file.sql
echo -en "备份数据SUCCESS.......$(date)"
echo -en "-----------------------"
