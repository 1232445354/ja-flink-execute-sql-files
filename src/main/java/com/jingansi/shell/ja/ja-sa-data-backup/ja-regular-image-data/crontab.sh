#!/bin/bash

# 定时同步图片数据

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-static-data

# 定时
* * * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-static-data/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-static-data/root.log




