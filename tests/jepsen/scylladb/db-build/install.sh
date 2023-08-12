#!/bin/bash
set -e # exit when any command fails

# Installing Fedora packages for llvm
dnf -y install llvm-devel vim curl

# Installing rustc
curl https://sh.rustup.rs -sSf | bash -s -- -y
echo "source $HOME/.cargo/env" >> "$HOME"/.bashrc
PATH="/root/.cargo/bin:${PATH}"
rustup default nightly-2022-02-14

# Building instrumentor
cd /opt/instrumentor/llvm_mode && make

# Downloading scylladb-5.0.12
cd /opt && git clone -b newer-jepsen-testing https://github.com/dranov/scylla.git scylla

# Locating targets
python3 /opt/instrumentor/tell_instr_targets.py --input . --output /opt/instrumentor/

cd /opt/scylla && git submodule update --init --recursive
# Configuring
export AFL_USE_ASAN=1 && ./configure.py --compiler /opt/instrumentor/llvm_mode/afl-clang-fast++ --with scylla --c-compiler /opt/instrumentor/llvm_mode/afl-clang-fast --static --mode debug
# Building
export AFL_USE_ASAN=1 && ninja -j "$(getconf _NPROCESSORS_ONLN)"
# Packing
ninja debug-dist

# Copying packages
mkdir /opt/packages && find ./ -name "*.deb" -exec cp -r "{}" /opt/packages/ \;
# Copying compiling info
cp /opt/instrumentor/BB2ID.txt /opt/packages/scylla_CodeLocToBBID.txt
cp /opt/instrumentor/instr-targets.txt /opt/packages/instr-targets.txt
cp /opt/instrumentor/targets-summary.txt /opt/packages/targets-summary.txt
