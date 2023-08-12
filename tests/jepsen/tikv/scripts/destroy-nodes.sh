#!/bin/bash
for i in {1..5}; do
  lxc-destroy -f -n n$i
done