#!/bin/bash

##########################################################################
# Execute this script to build db-server-v6.1.2 before running Jepsen tests 
# For some reason, this script doesn't work in the docker container
##########################################################################


#----------------------------------------------------------------------
# Requirements: clang-12/llvm-12, rustc-nightly-2021-02-14
# Others: cmake, make, curl, python3, zip
#----------------------------------------------------------------------

# Download binaries of pd-server, pd-ctl and tidb-server
rm -rf packages
mkdir -p packages/bin && cd packages/bin || exit
wget -c https://tiup-mirrors.pingcap.com/pd-v6.1.6-linux-amd64.tar.gz -O - | tar -xz
wget -c https://tiup-mirrors.pingcap.com/ctl-v6.1.6-linux-amd64.tar.gz -O - | tar -xz
cd ../..

docker run -it --privileged --name tikv-build -d docker.io/jgoerzen/debian-base-standard:buster bash

# Build subjects
docker cp ../../../../instrumentor tikv-build:/opt
docker cp ./install.sh tikv-build:/opt
docker exec tikv-build bash /opt/install.sh

# Copy binaries and delete files
docker cp tikv-build:/opt/tikv/target/debug/tikv-server ./packages/bin/tikv-server
docker cp tikv-build:/opt/instrumentor/instr-targets.txt ./packages/tikv-instr-targets.txt
docker cp tikv-build:/opt/instrumentor/targets-summary.txt ./packages/tikv-targets-summary.txt
docker cp tikv-build:/opt/instrumentor/BB2ID.txt ./packages/tikv-BB2ID.txt
docker stop tikv-build