#!/bin/bash
set -x
for i in {1..5}; do
  lxc-stop -k -n n$i
done