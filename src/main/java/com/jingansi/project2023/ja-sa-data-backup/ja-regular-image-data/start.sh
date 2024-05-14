#!/bin/bash

echo -en "开始备份互联网sa的图片表数据...$(date)\n"
mysql -h172.21.30.202 -P31030 -uadmin -pJingansi@110  sa < /data1/bigdata/apps/ja-sa-data-backup/ja-regular-image-data/sql_file.sql
echo -en "备份数据SUCCESS.......$(date)\n"
echo -en "-----------------------\n"
