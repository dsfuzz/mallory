#!/bin/bash
set -e # exit when any command fails

apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -qy cmake make curl python3 git ca-certificates wget perl sudo gdb

# install clang-12
echo "deb http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >> /etc/apt/sources.list
echo "deb-src http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >> /etc/apt/sources.list
wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt-get update -qy
apt-get install -qy clang-12 lld-12
ln -s /usr/bin/clang-12 /usr/bin/clang && ln -s /usr/bin/clang++-12 /usr/bin/clang++ && \
    ln -s /usr/bin/llvm-config-12 /usr/bin/llvm-config

# install rust-nightly-2022-02-14
curl https://sh.rustup.rs -sSf | bash -s -- -y
echo "source $HOME/.cargo/env" >> "$HOME"/.bashrc
PATH="/root/.cargo/bin:${PATH}"
rustup default nightly-2022-02-14

# download tikv
cd /opt && git clone -b newer-jepsen-testing https://github.com/mengrj/tikv.git

# Step1: Indetify interesting targets
python3 ./instrumentor/tell_instr_targets.py --input ./tikv --output ./instrumentor

# Step 2: (1) Build instrumentor, and (2) clear shared memory
cd ./instrumentor/llvm_mode && make && cd ../../tikv

# Step 3: (1) set up environment variables, and (2) build tikv
export TARGETS_FILE="/opt/instrumentor/instr-targets.txt" &&
export CC=clang && 
export CXX=clang++ &&
RUSTFLAGS="-g -Z new-llvm-pass-manager=no -Z llvm-plugins=/opt/instrumentor/llvm_mode/afl-llvm-pass.so -C passes=sancov -C codegen-units=1 -C llvm-args=-sanitizer-coverage-level=3 -C llvm-args=-sanitizer-coverage-trace-pc-guard -C llvm-args=-sanitizer-coverage-prune-blocks=0 -C opt-level=3 -C target-cpu=native -Clink-arg=-fuse-ld=gold -l afl-llvm-rt -L /opt/instrumentor/llvm_mode" cargo build