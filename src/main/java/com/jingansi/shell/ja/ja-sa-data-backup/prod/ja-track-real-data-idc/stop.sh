#!/bin/bash
ps aux | grep '/data1/bigdata/apps/ja-sa-data-backup/ja-track-real-data-idc' | grep -v grep | awk '{print $2}' | xargs kill -9

