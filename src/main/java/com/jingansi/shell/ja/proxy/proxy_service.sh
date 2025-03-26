cd /data1/apps/sa_base/proxy
source /data1/apps/sa_base/venv/bin/activate

echo -e "\n\nProxy service $(date '+%Y-%m-%d %H:%M:%S')..." >> logs/proxy_service.log
echo " Active python venv ..." >> logs/proxy_service.log
./proxy_fr24.sh >> logs/proxy_service.log 2>&1
./proxy_adsb.sh >> logs/proxy_service.log 2>&1
./proxy_radarbox.sh >> logs/proxy_service.log 2>&1
./proxy_radarbox_area.sh >> logs/proxy_service.log 2>&1