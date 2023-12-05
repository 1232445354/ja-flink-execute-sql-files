#!/bin/bash

# 静态数据,不变化的话,每个月同步一次都可以的

# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-history-data

# 定时
nohup sh start_ais_vessel.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-history-data/root1.log &