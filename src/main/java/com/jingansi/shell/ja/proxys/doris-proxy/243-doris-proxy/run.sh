#!/bin/bash
# /home/jingansi/doris-proxy/run.sh
nohup mysql-proxy --defaults-file=/data1/bigdata/apps/243-doris-proxy/doris-proxy.conf >>out.log 2>&1 &