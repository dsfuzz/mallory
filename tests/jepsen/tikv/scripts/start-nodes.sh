#!/bin/bash
set -x
for i in {1..5}; do
  lxc-start -d -n n$i
done