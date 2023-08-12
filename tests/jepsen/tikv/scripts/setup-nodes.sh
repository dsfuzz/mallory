#!/bin/bash
set -x
for i in {1..5}; do sudo lxc-destroy -f -n n$i; done
for i in {1..5}; do sudo lxc-create -n n$i -t centos; done

for i in {1..5}; do
sudo cat >>/var/lib/lxc/n${i}/config <<EOF
# Network config
lxc.network.type = veth
lxc.network.flags = up
lxc.network.link = virbr0
lxc.network.hwaddr = 00:1E:62:AA:AA:$(printf "%02x" $i)

# Configuration for fixing systemd-journal 100% cpu problem.
# Reference: https://lists.linuxcontainers.org/pipermail/lxc-users/2014-August/007600.html
lxc.kmsg = 0
EOF
done

for i in {1..5}; do
  mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh
  chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/
  cp ~/.ssh/id_rsa.pub /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
  chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
  # install start-stop-daemon
  cp ~/dpkg-1.17.27/utils/start-stop-daemon /var/lib/lxc/n${i}/rootfs/usr/bin/start-stop-daemon
done

for i in {1..5}; do
  lxc-start -d -n n$i
done
