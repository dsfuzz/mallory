#!/bin/bash
yum install -y centos-release-scl
yum install -y java ncurses-devel gcc-c++ gnuplot bind-utils \
                xorg-x11-server-Xvfb \
                devtoolset-7 \
                openssl-devel \
                cmake3 \
                graphviz

# install leiningen
mkdir -p ~/bin
cd ~/bin
curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x lein

# build start-stop-daemon in nodes
cd ~
wget "https://mirrors.tuna.tsinghua.edu.cn/debian/pool/main/d/dpkg/dpkg_1.17.27.tar.xz"
tar -xf dpkg_1.17.27.tar.xz
cd dpkg-1.17.27/
./configure
make

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 \
--family cmake
