#!/bin/bash

#mkdir -r /data1/bigdata/apps/ja-delete-error-pod
# */10 * * * * sh /data1/bigdata/apps/ja-delete-error-pod/delete-error-pod.sh > /data1/bigdata/apps/ja-delete-error-pod/root.log

*/5 * * * * sh /home/jingansi/k8s/init/bigdata-apps/delete-error-pod.sh > /home/jingansi/k8s/init/bigdata-apps/error-pod-root.log


