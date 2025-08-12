#!/bin/bash


0 1 * * * sh /data1/bigdata/apps/batch-processing/cross-area-alert/start.sh > /data1/bigdata/apps/batch-processing/cross-area-alert/root.log 2>&1


3 1 * * * sh /data1/bigdata/apps/batch-processing/stay-time-alert/start.sh > /data1/bigdata/apps/batch-processing/stay-time-alert/root.log 2>&1


