#!/bin/bash

#本地代理服务，绑定IP和端口
#注意：若部署在公网，IP必须是127.0.0.1/内网IP，或者代理端口配置成禁止外部访问
proxy_name="radarbox-area"
proxy_ip="172.21.30.203"
proxy_port=6666

#青果代理的ssh登陆账号、端口
#通过ssh证书免密码登陆（需要提前配置）
ssh_user="root"
ssh_ip="59.57.14.142"
ssh_port="20837"
ssh_key="./ssh_key/qg_ssh_key"

service_ip=""
service_port=""
service_key=""

# 1. 检查代理是否可访问
echo -e "\n1. Check proxy ... "
if curl -s -I -L  -x $proxy_ip:$proxy_port http://www.baidu.com | grep HTTP; then
    echo "Proxy $proxy_name[$proxy_ip:$proxy_port] is ok"
    exit 0
else
    echo "Proxy $proxy_name[$proxy_ip:$proxy_port] is not accessible."
fi

# 2. 执行远程ssh命令，提取当前proxy公网IP地址
echo -e "\n2. Fetch service_ip in remote server ... "
#cmd_result=$(ssh -p $ssh_port -i $ssh_key $ssh_user@$ssh_ip "curl -s www.cip.cc" | grep cip.cc)
cmd_result=$(ssh -p $ssh_port -i $ssh_key $ssh_user@$ssh_ip "curl -s ifconfig.me")
echo "cmd_result:$cmd_result"

service_ip=$(echo "$cmd_result" | cut -f 4 -d /)
echo "service_ip: ${service_ip}"

# 3. TODO：测试网络是否可用，若不可用执行拨号，并重新获取IP

# 4. 查询服务端proxy启动命令，查看端口和密钥
echo -e "\n4. Fetch service key/port in remote server ... "
cmd="ssh -p $ssh_port -i $ssh_key $ssh_user@$ssh_ip ps -elf | grep pproxy | grep ssr | cut -f 10- -d /"
cmd_result=$($cmd)
echo "cmd_result:$cmd_result"
service_port=$(echo $cmd_result | cut -f 3 -d :  )
service_key=$(echo $cmd_result | cut -f 2 -d : | cut -f 1 -d @)
echo "service_port: ${service_port}"
echo "service_key: ${service_key}"

# 5. kill本地代理服务
echo -e "\n5. Kill pproxy service if running ... "
pproxy_pid=$(ps -elf | grep pproxy | grep chacha  | grep ${proxy_port} | sed 's/  */ /g' | cut -f 4 -d ' ')
echo "pproxy_id:$pproxy_pid"
if [[ ! $pproxy_pid =~ ^[[:space:]]+$ ]]; then
    echo " kill pid:$pproxy_pid"
    cmd_result=$(kill -9 $pproxy_pid)
fi

# 6. 重新启动pproxy
echo -e "\n6. Start pproxy service ... "
cmd="nohup pproxy -l http+socks4+socks5://${proxy_ip}:${proxy_port} -r ssr://chacha20:${service_key}@${service_ip}:${service_port} &"
echo "start pproxy ${cmd} ..."
nohup /usr/local/bin/pproxy -l http+socks4+socks5://${proxy_ip}:${proxy_port} -r ssr://chacha20:${service_key}@${service_ip}:${service_port} &
sleep 3

# 7. 再次检查 www.baidu.com 是否可访问
echo -e "\n6. Recheck pproxy ... "
if curl -s -I -L  -x $proxy_ip:$proxy_port http://www.baidu.com | grep HTTP; then
    echo "Proxy[$proxy_ip:$proxy_port] is ok"
    exit 0
else
    echo "Proxy[$proxy_ip:$proxy_port] is not accessible."
fi