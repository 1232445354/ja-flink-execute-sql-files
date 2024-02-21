#!/bin/bash

# 静态数据,不变化的话,每个月同步一次都可以的

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-real-data

# 定时
1 3 * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log


nohup sh start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-real-data/root.log &