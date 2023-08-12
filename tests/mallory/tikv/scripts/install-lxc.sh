#!/bin/bash
# Reference: https://www.tecmint.com/install-create-run-lxc-linux-containers-on-centos/
yum install -y epel-release
yum install -y debootstrap perl libvirt
yum install -y lxc lxc-templates
systemctl start lxc
systemctl start libvirtd
