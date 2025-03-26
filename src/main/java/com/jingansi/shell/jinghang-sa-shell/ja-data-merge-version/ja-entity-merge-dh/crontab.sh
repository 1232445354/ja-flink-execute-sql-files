#!/bin/bash
# 聚合数据脚本

# 部署警航服务器
40 * * * * sh /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-dh/start.sh > /data1/bigdata/apps/ja-data-merge-version/ja-entity-merge-dh/root.log 2>&1