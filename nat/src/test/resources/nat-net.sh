#!/bin/bash
#
# This will setup a NAT environment in your local machine. Using namespaces, one
# can setup different NAT for testing on a single machine. Alternatives are
# using real hardware or virtual machines. The network setup looks as follows:
#
# -------------------------------------------------------------
#           +--------+  +------------+
# global    |DEV_REAL|--|NAT_REAL    |
# namespace |your IP |  |192.168.1.50|
#           +--------+  +-----+------+
# ----------------------------|--------------------------------
#                       +-----+----+  +--------+
# natX                  |NAT_WAN   |--|NAT_LAN |
# namespace             |172.20.0.1|  |10.0.0.1|
#                       +----------+  +----+---+
# -----------------------------------------|-------------------
#                                     +----+---+  +----+---+
# unrX                                |UNR_LAN1|--|UNR_LAN2|
# namespace                           |10.0.0.2|  |10.0.0.3|
#                                     +--------+  +--------+
#--------------------------------------------------------------
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
# port forwarding, will forward the port in the natX namespace
# > nat-net.sh forward 0 4000 10.0.0.2
#
# start UPNP deamon in the natX namespace with the external interface NAT_WAN and internal interface NAT_LAN
# > nat-net.sh upnp 0
#
# Using the default values, your relay/rendez-vous peer should listen on address 192.168.1.50,
# while the unreachable peer should listen on address 10.0.0.2. The unreachable peer needs to
# be called using the unrX namespace, e.g., "ip netns exec unr1 java PeerClass".
#
# Most likely, you will not have permission to run ip as non-root. Add the following lines to sudoers:
# username ALL=(ALL) NOPASSWD: <location>/nat-net.sh
# username ALL=(ALL) NOPASSWD: /usr/bin/ip # or /sbin/ip as in Ubuntu
#
# Make sure the network namespaces can resolve the hostname, otherwise huge delays
# are to be expected. E.g., logback could hang for a minute. By adding the hostname
# to /etc/hosts resolved this issue.
#
# For half configured nat, use this to clean
# ps ax | grep "sudo ip" | cut -d" " -f2 | sudo xargs kill
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
UNR_LAN1="10.0.$2.2"
UNR_LAN2="10.0.$2.3"

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
  ip netns exec "unr$1" ifconfig "unr$1_lan" "$UNR_LAN1"/24 up
  
  # adding a virtual interface to simulate two peers behind same NAT
  #
  ip netns exec "unr$1" ifconfig "unr$1_lan:0" "$UNR_LAN2"/24 up 
  # alternatively, one can use a tun device
  #ip netns exec "unr$1" ip tuntap add dev "unr$1_lan2" mode tun
  #ip netns exec "unr$1" ifconfig "unr$1_lan2" "$UNR_LAN2"/24 up
  
  # loopback is important or getLocalHost() will hang for a long time!
  ip netns exec "unr$1" ifconfig "lo" 127.0.0.1 up
  
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
  killall -q miniupnpd
}

forward () {
  ip netns exec "nat$1" iptables -t nat -A PREROUTING -i nat$1_wan -p tcp --dport $2 -j DNAT --to-destination $3:$2
  ip netns exec "nat$1" iptables -t nat -A PREROUTING -i nat$1_wan -p udp --dport $2 -j DNAT --to-destination $3:$2
  ip netns exec "nat$1" iptables -A FORWARD -d $3 -p tcp --dport $2 -m state --state NEW,ESTABLISHED,RELATED -j ACCEPT
  ip netns exec "nat$1" iptables -A FORWARD -d $3 -p udp --dport $2 -m state --state NEW,ESTABLISHED,RELATED -j ACCEPT
}

upnp () {
  ip netns exec "nat$1" iptables -t nat -N MINIUPNPD
  ip netns exec "nat$1" iptables -t nat -I PREROUTING -j MINIUPNPD
  ip netns exec "nat$1" iptables -t filter -N MINIUPNPD
  ip netns exec "nat$1" iptables -t filter -I FORWARD -j MINIUPNPD
  ip netns exec "nat$1" miniupnpd -i nat$1_wan -a nat$1_lan
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
  
  forward)
    forward $2 $3 $4
  ;;
  
  upnp)
    upnp $2
  ;;
  
  *)
    echo $"Usage: $0 {start|stop|restart} nr [sym]"
    exit 1

esac
