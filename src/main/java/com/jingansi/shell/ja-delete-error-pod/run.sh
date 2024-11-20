#!/bin/bash

kubectl get pod -nja |grep ja-pipeline-app-dag|awk '$3 =="Error" {print $1,$3}' |xargs kubectl delete pod -nja
