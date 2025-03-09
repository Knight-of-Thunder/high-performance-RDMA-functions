#!/bin/bash

# 提示用户输入设备名和网卡名
read -p "请输入设备名（例如 rxe0）: " device_name
read -p "请输入网卡名（例如 eth0）: " netdev_name

# 显示用户输入的设备名和网卡名
echo "您输入的设备名是: $device_name"
echo "您输入的网卡名是: $netdev_name"

# 检查系统是否支持 RDMA
if grep -q RXE /boot/config-$(uname -r); then
    echo "系统支持 RDMA。"
else
    echo "系统不支持 RDMA，请检查内核配置。"
    exit 1
fi

# 安装必要的 RDMA 软件包
echo "正在安装必要的 RDMA 软件包..."
sudo apt-get update
sudo apt-get install -y libibverbs1 ibverbs-utils librdmacm1 libibumad3 ibverbs-providers rdma-core

# 加载 rdma_rxe 模块
echo "正在加载 rdma_rxe 模块..."
sudo modprobe rdma_rxe

# 添加 RXE 设备
echo "正在添加 RXE 设备..."
sudo rdma link add "$device_name" type rxe netdev "$netdev_name"

# 显示 RDMA 链接信息
echo "当前的 RDMA 链接信息："
rdma link
