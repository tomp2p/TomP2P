#!/bin/bash
#
# This will setup a NAT environment in your local machine. Using namespaces, one
# can setup different NAT for testing on a single machine. Alternatives are
# using real hardware or virtual machines. The network setup looks as follows:
#
# --------------------------------------------------
#           +--------+  +------------+
# global    |DEV_REAL|--|NAT_REAL    |
# namespace |your IP |  |192.168.1.50|
#           +--------+  +-----+------+
# ----------------------------|---------------------
#                       +-----+----+  +--------+
# natX                  |NAT_WAN   |--|NAT_LAN |
# namespace             |172.20.0.1|  |10.0.0.1|
#                       +----------+  +----+---+
# -----------------------------------------|--------
#                                     +----+---+
# unrX                                |UNR_LAN |
# namespace                           |10.0.0.2|
#                                     +--------+
#---------------------------------------------------
#
# Two tyes of NAT are supported, symmetric NAT with port prediction and 
# symmetric NAT with random port assigned (as in FreeBSD). Example to test the NAT 
# using the following commands:
#
# setup first NAT. Numbers from 0-9 are supported to create 10 NAT.
# > nat-net.sh start 0  
#
# setup symmetric NAT (random ports)
# > nat-net.sh start 1 sym
# 
# ping the address in the global namespace, which traverses NAT
# > ip netns exec unr0 ping 192.168.1.51 
#
# see the NAT table
# > ip netns exec nat0 conntrack -L
# 
# connect with TCP
# > nc -l  192.168.1.50 5555
# > ip netns exec unr0 nc -s 10.0.0.2 192.168.1.50 5555
#
# stop NAT
# > nat-net.sh stop 0
#
# Using the default values, your relay/rendez-vous peer should listen on address 192.168.1.50,
# while the unreachable peer should listen on address 10.0.0.2. The unreachable peer needs to
# be called using the unrX namespace, e.g., "ip netns exec unr1 java PeerClass".
#
# Author: Thomas Bocek

# Set the IP address to something in your subnet of your global namespace
NAT_REAL="192.168.1.5$2"
NAT_REAL_NET="192.168.1.0"
# IP address should not be in a subnet of your global namespace,
# as you would need an additional ARP proxy
NAT_WAN="172.20.$2.1"
NAT_WAN_NET="172.20.$2.0"
NAT_LAN="10.0.$2.1"
UNR_LAN="10.0.$2.2"

start () {
  # create 2 namespaces: unreachable and nat
  ip netns add "unr$1"
  ip netns add "nat$1"
  # setup virtual interfaces
  ip link add "nat$1_real" type veth peer name "nat$1_wan"
  ip link set "nat$1_wan" netns "nat$1"
  ip link add "nat$1_lan" type veth peer name "unr$1_lan"
  ip link set "nat$1_lan" netns "nat$1"
  ip link set "unr$1_lan" netns "unr$1"
  echo "nat$1_real, nat$1_wan created in namespace nat$1." 
  echo "nat$1_lan,  unr$1_lan created in namespace unr$1." 
  # assign IPs
  ifconfig "nat$1_real" "$NAT_REAL"/24 up
  ip netns exec "nat$1" ifconfig "nat$1_wan" "$NAT_WAN"/24 up
  ip netns exec "nat$1" ifconfig "nat$1_lan" "$NAT_LAN"/24 up
  ip netns exec "unr$1" ifconfig "unr$1_lan" "$UNR_LAN"/24 up
  # add, modify routing
  route del -net "$NAT_REAL_NET"/24 dev "nat$1_real"
  route add -net "$NAT_WAN_NET"/24 dev "nat$1_real"
  ip netns exec "unr$1" route add default gw "$NAT_LAN"
  ip netns exec "nat$1" route add default gw "$NAT_WAN"
  echo "routing set."
  # setup NAT
  ip netns exec "nat$1" echo 1 > /proc/sys/net/ipv4/ip_forward

  if [ -z "$2" ]; then 
    ip netns exec "nat$1" iptables -t nat -A POSTROUTING -o "nat$1_wan" -j MASQUERADE
  else 
    ip netns exec "nat$1" iptables -t nat -A POSTROUTING -o "nat$1_wan" -j MASQUERADE --random
  fi

  ip netns exec "nat$1" iptables -A FORWARD -i "nat$1_wan" -o "nat$1_lan" -m state --state RELATED,ESTABLISHED -j ACCEPT
  ip netns exec "nat$1" iptables -A FORWARD -i "nat$1_lan" -o "nat$1_wan" -j ACCEPT
  echo "NAT setup $2".
}

stop () {
  # deleting the namespace should remove all the config from above
  ip netns del "unr$1"
  ip netns del "nat$1"
  ip link del "nat$1_real"
}

case "$1" in
  start)
    start $2 $3
  ;;
         
  stop)
    stop $2
  ;;

  restart)
    stop $2
    start $2 $3
  ;;

  *)
    echo $"Usage: $0 {start|stop|restart} nr [sym]"
    exit 1

esac
