#!/bin/bash
virsh net-destroy default

cp /etc/resolv.conf /etc/resolv.conf.old
grep -v 192.168.122.1 /etc/resolv.conf.old > /etc/resolv.conf
rm -f /etc/resolv.conf.old