#!/bin/bash

# We add our hostname to the shared volume, so that control can find us
echo "Adding hostname to shared volume" >> /var/log/jepsen-setup.log
hostname >> /var/jepsen/shared/nodes

# Copy instrumentation IDs to shared volume
cp /opt/instrumentor/*_CodeLocToBranchID.txt /var/jepsen/shared/
cp /opt/instrumentor/*_FuncNameToID.txt /var/jepsen/shared/

# We make sure that root's authorized keys are ready
echo "Setting up root's authorized_keys" >> /var/log/jepsen-setup.log
mkdir /root/.ssh
chmod 700 /root/.ssh
cp /run/secrets/authorized_keys /root/.ssh/
chmod 600 /root/.ssh/authorized_keys
