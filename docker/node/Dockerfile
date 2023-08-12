# See https://github.com/jgoerzen/docker-debian-base
# See https://hub.docker.com/r/jgoerzen/debian-base-standard
FROM docker.io/jgoerzen/debian-base-standard:buster

# I think this is a bug--debian-base-setup crashes because policy-rc.d isn't
# present in this image, and if you create it, exim crashes... do we actually NEED this? Maybe not...
#RUN mkdir /usr/sbin/policy-rc.d
#RUN run-parts --exit-on-error --verbose /usr/local/debian-base-setup

# Basic system stuff
RUN apt-get update
RUN apt-get install -qy apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install dos2unix openssh-server pwgen iptables traceroute netcat \ 
    nmap tshark tcpdump libpcap-dev silversearcher-ag

# When run, boot-debian-base will call this script, which does final
# per-db-node setup stuff.
ADD setup-jepsen.sh /usr/local/preinit/03-setup-jepsen
RUN chmod +x /usr/local/preinit/03-setup-jepsen
# Just to make sure; line endings errors lead to a failed boot
RUN dos2unix /usr/local/preinit/03-setup-jepsen

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH enabled

# Install Jepsen deps
RUN apt-get update -qy
RUN apt-get install -qy build-essential bzip2 dnsutils faketime iproute2 iptables iputils-ping \ 
    libzip4 logrotate man man-db net-tools ntpdate psmisc python rsyslog sudo tar unzip vim \ 
    wget ca-certificates python3 python3-pip gcc g++ pkg-config

# Set up firewall rules for custom routing
ADD setup-networking.sh /etc/rc.local
RUN chmod +x /etc/rc.local

EXPOSE 22

# build tools
RUN apt-get install -qy libuv1-dev liblz4-dev autoconf libtool make pkg-config git automake
RUN echo "deb http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >> /etc/apt/sources.list
RUN echo "deb-src http://apt.llvm.org/buster/ llvm-toolchain-buster-12 main" >> /etc/apt/sources.list
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-get update -qy
RUN apt-get install -qy clang-12 lld-12
RUN ln -s /usr/bin/clang-12 /usr/bin/clang && ln -s /usr/bin/clang++-12 /usr/bin/clang++ && \
    ln -s /usr/bin/llvm-config-12 /usr/bin/llvm-config
# end build tools

# Install coverage dependencies
WORKDIR /opt
RUN apt-get install -y valgrind tmux gdb libuv1-dev liblz4-dev autoconf git\
    libtool make pkg-config

#########################
# Dependencies for SUTs #
#########################

# install BRPC
ENV CXX=clang++
ENV CC=clang
WORKDIR /opt/fs
RUN apt-get install -y git g++ make libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libsnappy-dev cmake libgtest-dev
RUN git clone https://github.com/apache/incubator-brpc.git brpc
WORKDIR /opt/fs/brpc
RUN apt-get install -y libgoogle-glog-dev
RUN bash config_brpc.sh --headers=/usr/include --libs=/usr/lib --with-glog && make -j$(getconf _NPROCESSORS_ONLN)
RUN cp -r /opt/fs/brpc/output/* /

# Instrumentor

WORKDIR /opt
ENV CXX=clang++
ENV CC=clang
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup default stable
# Hack: update crates.io now so it's not updated on every build
RUN cargo install empty-library || true
COPY jepsen/instrumentor instrumentor
# This is ugly: we need to copy raw_packet.rs because Docker doesn't follow symlinks outside build context
RUN cd instrumentor/llvm_mode; make

# end build tools

######################
# SYSTEMS UNDER TEST #
######################

ENV CXX=/opt/instrumentor/llvm_mode/afl-clang-fast++
ENV CC=/opt/instrumentor/llvm_mode/afl-clang-fast

# IMPORTANT: To generate the function call instrumentation correctly within Docker, all 
# the compilation related to a given test subject MUST run within the same RUN instruction.

# Baidu Raft
WORKDIR /opt/fs/
ENV CXX=/opt/instrumentor/llvm_mode/afl-clang-fast++
ENV CC=/opt/instrumentor/llvm_mode/afl-clang-fast
# braft: 1ef36c2(Mar17,2023)
RUN git clone -b newer-jepsen-testing https://github.com/dranov/braft.git
WORKDIR /opt/fs/braft
RUN python3 /opt/instrumentor/tell_instr_targets.py --input ./src --output .
RUN mkdir bld && cd bld && cmake -DBRPC_WITH_GLOG=ON .. \
    && export TARGETS_FILE=/opt/fs/braft/instr-targets.txt \
    && export AFL_USE_ASAN=1 \
    && make -j$(getconf _NPROCESSORS_ONLN) \
    # install BRaft & braft_cli
    && cp -r /opt/fs/braft/bld/output/* / \
    && cp -r /opt/fs/braft/bld/tools/output/* / \
    && cd /opt/fs/braft/example/atomic \
    && cmake . && export TARGETS_FILE=/opt/fs/raft-c/instr-targets.txt \
    && export AFL_USE_ASAN=1 \
    && make -j$(getconf _NPROCESSORS_ONLN)
RUN mv /opt/instrumentor/BB2ID.txt /opt/instrumentor/braft_CodeLocToBBID.txt || true
COPY jepsen/tests/mallory/braft/resources/jepsen_control.sh /opt/fs/braft/example/atomic/jepsen_control.sh

# Dqlite (based on Canonical Raft, which we build again from upstream)
WORKDIR /opt/fs
ENV CXX=clang++
ENV CC=clang

RUN apt-get install -y libsqlite3-dev libuv1-dev
# raft-c: v0.17.1, dqlite: v1.14.0
RUN git clone -b newer-jepsen-testing https://github.com/mengrj/raft-c.git dqlite-raft
RUN git clone -b newer-jepsen-testing https://github.com/mengrj/dqlite.git dqlite

ENV DQLITE_VER=v1.11.7

WORKDIR /opt/fs
RUN cd dqlite-raft \
    && python3 /opt/instrumentor/tell_instr_targets.py --input ./src --output .

WORKDIR /opt/fs
RUN cd dqlite \
    && python3 /opt/instrumentor/tell_instr_targets.py --input ./src --output .

# Build everything in one go (required to correctly generate instrumentation)
WORKDIR /opt/fs

ENV CXX=/opt/instrumentor/llvm_mode/afl-clang-fast++
ENV CC=/opt/instrumentor/llvm_mode/afl-clang-fast

RUN cd dqlite-raft \
    && export TARGETS_FILE=/opt/fs/dqlite-raft/instr-targets.txt \
    && autoreconf -i \
    && ./configure --enable-debug --enable-sanitize \
    && make -j$(getconf _NPROCESSORS_ONLN) \
    && make install \
    && cd ../dqlite \
    && export TARGETS_FILE=/opt/fs/dqlite/instr-targets.txt \
    && autoreconf -i \
    && ./configure --enable-debug --enable-sanitize \
    && make -j$(getconf _NPROCESSORS_ONLN) \
    && make install
RUN mv /opt/instrumentor/BB2ID.txt /opt/instrumentor/dqlite_CodeLocToBBID.txt || true

# This fixes some certificate-related issues (by installing a new version of GnuTLS?)
RUN apt-get update -y
RUN apt-get upgrade -y
# We install a recent version of Go
WORKDIR /root
RUN curl -L -O https://go.dev/dl/go1.18.4.linux-amd64.tar.gz \
    && tar xvf go1.18.4.linux-amd64.tar.gz \
    && chown -R root:root ./go \
    && mv go /usr/local
RUN echo "export PATH=$PATH:/usr/local/go/bin" >> /root/.bashrc
ENV PATH="$PATH:/usr/local/go/bin" 

COPY jepsen/tests/mallory/dqlite/resources/app.go /opt/fs/dqlite/app.go
WORKDIR /opt/fs/dqlite

ENV CXX=clang++
ENV CC=clang
RUN go mod init app
RUN go get -tags libsqlite3 github.com/canonical/go-dqlite/app@${DQLITE_VER}
RUN export CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
    && go build -o app -asan app.go

# RedisRaft
WORKDIR /opt
ENV CXX=clang++
ENV CC=clang
RUN mkdir /opt/redis
WORKDIR /opt/fs
# redis: 7.0.10; redisraft: 7b460794(Arp4,2023)
RUN git clone -b jepsen-unstable https://github.com/mengrj/redis.git
RUN git clone -b newer-jepsen-testing https://github.com/mengrj/redisraft.git
WORKDIR /opt/fs
RUN cd redis && python3 /opt/instrumentor/tell_instr_targets.py --input ./src --output .

WORKDIR /opt/fs
RUN cd redisraft && python3 /opt/instrumentor/tell_instr_targets.py --input ./src --output .

ENV CXX=/opt/instrumentor/llvm_mode/afl-clang-fast++
ENV CC=/opt/instrumentor/llvm_mode/afl-clang-fast

WORKDIR /opt/fs
RUN cd redis && make distclean \
    && export TARGETS_FILE=/opt/fs/redis/instr-targets.txt \
    && make -j$(getconf _NPROCESSORS_ONLN) SANITIZER=address \
    && cd ../redisraft && mkdir build && cd build \
    && export TARGETS_FILE=/opt/fs/redisraft/instr-targets.txt \
    && cmake .. -DSANITIZER=address \
    && make -j$(getconf _NPROCESSORS_ONLN)
RUN mv /opt/instrumentor/BB2ID.txt /opt/instrumentor/redisraft_CodeLocToBBID.txt || true
RUN cp /opt/fs/redis/src/redis-server /opt/redis/
RUN chmod +x /opt/redis/redis-server
RUN cp /opt/fs/redis/src/redis-cli /opt/redis/
RUN chmod +x /opt/redis/redis-cli
RUN cp /opt/fs/redisraft/redisraft.so /opt/redis
# end RedisRaft

# # MongoDB
# WORKDIR /opt/fs
# RUN apt-get -y update
# RUN apt-get -y install libatomic1
# COPY jepsen/tests/mallory/mongodb/db-build/packages/mongod /usr/bin/
# COPY jepsen/tests/mallory/mongodb/db-build/packages/mongos /usr/bin/
# RUN chmod +x /usr/bin/mongod
# RUN chmod +x /usr/bin/mongos
# RUN mkdir /var/log/mongodb/
# RUN touch /var/log/mongodb/mongod.log
# RUN mkdir /var/lib/mongodb
# COPY jepsen/tests/mallory/mongodb/db-build/packages/mongo_*.txt /opt/instrumentor

# # ScyllaDB

# WORKDIR /opt/
# ENV CXX=clang++
# ENV CC=clang

# RUN curl -sL "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" | apt-key add - && \
#     echo "deb  [arch=amd64] https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main" > /etc/apt/sources.list.d/adoptopenjdk.list && \
#     apt-get update && \
#     apt-get install -qy adoptopenjdk-8-hotspot-jre python2

# # To build and get scylladb packages, first enter tests/scylladb and run build.sh script.
# WORKDIR /opt/
# COPY jepsen/tests/mallory/scylladb/db-build/packages packages
# WORKDIR /opt/packages
# RUN find . -name "scylla-python3_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-conf_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-server_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-jmx_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-kernel-conf_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-tools-core_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-tools_*" -exec dpkg -i {} \;
# RUN find . -name "scylla-node-exporter_*" -exec dpkg -i {} \;
# RUN find . -name "scylla_*.deb" -exec dpkg -i {} \;

# RUN cp scylla*.txt /opt/instrumentor/

# RUN rm -r /opt/packages

# Workaround SystemD timeout: https://docs.scylladb.com/stable/troubleshooting/scylla-wont-start.html
# RUN printf '[Service]\nTimeoutStartSec=120\n'> /etc/systemd/system/scylla-server.service.d/10-timeout.conf
# Make the SystemD service spawn Scylla under the cov-server.py
# COPY jepsen/tests/mallory/scylladb/resources/systemd/scylla-server.service /lib/systemd/system/scylla-server.service

# # TiKV/TiDB
# COPY jepsen/tests/mallory/tikv/db-build/packages/ /opt/fs/tidb
# WORKDIR /opt/fs/tidb
# RUN chmod +x ./bin/*
# RUN cp /opt/fs/tidb/tikv-*.txt /opt/instrumentor/

# end systems

# coverage server
WORKDIR /opt
ENV CXX=clang++
ENV CC=clang
RUN mkdir shm

# Hack: update crates.io now so it's not updated on every build
RUN cargo install empty-library || true
# Cache dependencies: compile with a dummy main, then replace it with the real one
COPY jepsen/cov-server/Cargo.toml cov-server/Cargo.toml
RUN cd / && \
    cargo new playground\
    && cd /opt && cp -r /playground/src cov-server/src
RUN cd cov-server && RUSTFLAGS="-C target-cpu=native" cargo build --release
RUN cd cov-server && cargo clean -p cov-server && rm -rf src/

# Actually build
COPY jepsen/cov-server/ cov-server/
RUN cd cov-server && \
    # Update the timestamps of all files to force a rebuild
    find ./src/ -type f -name "*.rs" -exec touch {} + && \
    RUSTFLAGS="-C target-cpu=native" cargo build --release
WORKDIR /opt/cov-server
COPY jepsen/cov-server/Coverage.toml ./target/release/Coverage.toml

EXPOSE 8080

RUN update-alternatives --set iptables /usr/sbin/iptables-legacy
RUN update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# end coverage

CMD ["/usr/local/bin/boot-debian-base"]
