#!/bin/bash

# 部署204服务器

0 2 * * * nohup sudo sh /opt/bigdata/sync-jinghang/ja-minio-file/run.sh  > /opt/bigdata/sync-jinghang/ja-minio-file/out.log 2>&1 &


