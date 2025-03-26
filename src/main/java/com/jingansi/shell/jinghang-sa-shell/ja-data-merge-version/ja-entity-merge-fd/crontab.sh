#!/bin/bash
# 聚合数据脚本

# 部署警航服务器
50 00 * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-fd/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-fd/root.log 2>&1


