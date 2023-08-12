#!/bin/bash
set -x
for i in {1..5}; do
  lxc-attach -n n${i} -- bash -c 'echo -e "jepsen-tikv\njepsen-tikv\n" | passwd root';
  lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  lxc-attach -n n${i} -- systemctl restart sshd;
done

rm -f ~/.ssh/known_hosts
for n in {1..5}; do ssh-keyscan -t rsa n$n; done >> ~/.ssh/known_hosts

for i in {1..5}; do
  lxc-attach -n n${i} -- yum install -y sudo
done