
ps -ef |grep "ja-acquire-images/ja-sentinel2/2025"|grep -v grep |awk '{print $2}'|xargs kill -9