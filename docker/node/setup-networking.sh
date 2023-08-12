#!/bin/sh -e

NODEID=`hostname | sed 's/n//'`
export NODEID
# ip route delete default
# ip route add default via 172.16.0.1
ip route add 10.1.0.0/16 via 10.1.${NODEID}.2
# iptables-legacy -A OUTPUT -d 172.16.0.1/30 -j ACCEPT
# iptables-legacy -A OUTPUT -d 172.16.0.1/24 -j DROP
echo "Done setting firewall rules for n${NODEID}" >> /var/log/jepsen-setup.log

exit 0