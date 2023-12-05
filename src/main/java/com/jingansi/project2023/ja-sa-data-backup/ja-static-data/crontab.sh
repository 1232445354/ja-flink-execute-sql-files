#!/bin/bash

# 静态数据：一次同步 或者一个月同步一次
# 静态数据分类：部分的表同步，部分的表不能同步（poi同步没必要）


# 部署位置 : 172.21.30.201
	/data1/bigdata/apps/ja-sa-data-backup/ja-static-data

# 定时
* * * * * sh /data1/bigdata/apps/ja-sa-data-backup/ja-static-data/start.sh > /data1/bigdata/apps/ja-sa-data-backup/ja-static-data/root.log
