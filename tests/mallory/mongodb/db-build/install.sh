#!/bin/bash
set -e # exit when any command fails

# Install packages for instrumentation
apt-get -y update
apt-get -qy install make cmake curl wget ca-certificates

# install clang-12
echo "deb http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >>/etc/apt/sources.list
echo "deb-src http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >>/etc/apt/sources.list
wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt-get update -qy
apt-get install -qy clang-12 lld-12
ln -s /usr/bin/clang-12 /usr/bin/clang && ln -s /usr/bin/clang++-12 /usr/bin/clang++ &&
    ln -s /usr/bin/llvm-config-12 /usr/bin/llvm-config

# install rust-nightly-2022-02-14
curl https://sh.rustup.rs -sSf | bash -s -- -y
echo 'source $HOME/.cargo/env' >>$HOME/.bashrc
PATH="/root/.cargo/bin:${PATH}"
rustup default nightly-2022-02-14

# Build instrumentor
cd /opt/instrumentor/llvm_mode && make

# Build mongodb-6.0.5
apt-get install -y libcurl4-openssl-dev liblzma-dev python3 python3-dev python3-pip libssl-dev git
cd /opt && git clone -b newer-jepsen-testing https://github.com/dranov/mongo.git
# Configuring
cd /opt/mongo
python3 -m pip install -r etc/pip/compile-requirements.txt
# locate targets
python3 /opt/instrumentor/tell_instr_targets.py --input ./src/mongo/ --output /opt/instrumentor/
# Building
export AFL_USE_ASAN=1 && python3 buildscripts/scons.py CC=/opt/instrumentor/llvm_mode/afl-clang-fast CXX=/opt/instrumentor/llvm_mode/afl-clang-fast++ install-mongod --disable-warnings-as-errors -j$(getconf _NPROCESSORS_ONLN) \
&& python3 buildscripts/scons.py CC=clang CXX=clang++ install-mongos --disable-warnings-as-errors -j$(getconf _NPROCESSORS_ONLN)
# Packing: command seems incorret 
# python3 buildscripts/packager.py -d debian10 -p /opt/mongo/packages -s 6.0.0 -t /opt/mongo/build/install/bin/mongod

# Copying packages
mkdir /opt/packages
cp /opt/mongo/build/install/bin/mongod /opt/packages
cp /opt/mongo/build/install/bin/mongos /opt/packages
# Copying compiling info
cp /opt/instrumentor/BB2ID.txt /opt/packages/mongo_CodeLocToBBID.txt
cp /opt/instrumentor/instr-targets.txt /opt/packages/instr-targets.txt
cp /opt/instrumentor/targets-summary.txt /opt/packages/targets-summary.txt
