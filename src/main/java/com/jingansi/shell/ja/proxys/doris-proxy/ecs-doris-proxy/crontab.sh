#!/bin/bash

<<comment

[mysql-proxy]
proxy-address = :31032
proxy-backend-addresses = 121.196.192.212:31030

注释解释:
  1.这个程序启动在哪一个服务器上，那么这一台服务器的proxy-address 端口都将被代理到这个地址上 proxy-backend-addresses
  2.假设这个服务器IP为172.21.30.241
    那么访问172.21.30.241:31032的请求，将会被代理到121.196.192.212:31030

comment



