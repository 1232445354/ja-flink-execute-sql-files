source ${DIR}/config.sh
echo -en "开始按小时聚合生成数据...$(date)\n"
cur_hour_time=$(date +"%Y-%m-%d %H:%M:%S")


sql="

select * from sa.xxx

"


mysql -h${host} \
-P${port} \
-u${username} \
-p${password} \
-e "${sql}"



