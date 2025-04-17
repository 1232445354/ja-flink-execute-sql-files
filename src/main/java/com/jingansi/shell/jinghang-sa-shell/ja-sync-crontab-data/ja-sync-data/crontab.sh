#!/bin/bash

# 部署警航的135.100.11.132 服务器

10 2 * * * nohup sudo sh /data1/bigdata/apps/sync-jinghang/ja-sync-data/start.sh  > /data1/bigdata/apps/sync-jinghang/ja-sync-data/root.log 2>&1 &

