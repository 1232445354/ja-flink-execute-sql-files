#!/bin/bash

# 部署204服务器

10 2 * * * nohup sudo sh /opt/bigdata/sync-jinghang/ja-sync-data/start.sh  > /opt/bigdata/sync-jinghang/ja-sync-data/root.log 2>&1 &


