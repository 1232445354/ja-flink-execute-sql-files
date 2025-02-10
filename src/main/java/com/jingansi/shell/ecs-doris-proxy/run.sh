#!/bin/bash
# /home/jingansi/doris-proxy/delete-error-pod.sh
nohup mysql-proxy --defaults-file=/home/jingansi/ecs-doris-proxy/doris-proxy.conf >>out.log 2>&1 &