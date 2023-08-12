FROM debian:buster
ENV DEBIAN_FRONTEND=noninteractive
ENV LEIN_ROOT true

#
# Jepsen dependencies and other dependencies
#
RUN apt-get -y -q update
RUN apt-get install -qy openjdk-11-jdk-headless \
    libjna-java \
    vim \
    emacs \
    git \
    htop \
    screen \
    pssh \
    curl \
    wget \
    gnuplot \
    graphviz \
    dos2unix \
    iptables traceroute netcat nmap tshark tcpdump net-tools procps\
    build-essential

# install TiKV
RUN apt-get install -qy libssl-dev cmake

# This is needed for setting up routing
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy
RUN update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# The link is unaccessiable sometimes. So we install lein using apt
# RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
#     mv lein /usr/bin && \
#     chmod +x /usr/bin/lein && \
#     lein self-install
RUN apt install -y leiningen

# Configure our own DNS server
# (I couldn't figure out how to add entries to Docker's built-in one)
# dnsmasq automatically reads from /etc/hosts to set up DNS entries
# see: https://somoit.net/linux/configure-simple-dns-server
RUN apt-get install -y dnsmasq dnsutils && systemctl enable dnsmasq

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init.sh && \
    chmod +x /tmp/rustup-init.sh && \
    /tmp/rustup-init.sh -y
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"

# For data analysis
RUN apt-get update -qy
RUN apt-get install -qy python3 python3-pip silversearcher-ag
RUN pip3 install --upgrade pip
RUN pip3 install pandas matplotlib

# Install Passive Observer Jepsen
RUN cd / && git clone https://github.com/dranov/jepsen.git jepsen-passive && echo 'ca633da'
RUN cd /jepsen-passive && git checkout passive-observation && cd jepsen && lein install

# without --dev flag up.sh copies jepsen to these subfolders
# with --dev flag they are empty until mounted

# # we don't want to download the dependencies on every run
COPY jepsen/jepsen/project.clj /jepsen/jepsen/project.clj
RUN if [ -f /jepsen/jepsen/project.clj ]; then cd /jepsen/jepsen && lein deps; fi

# actually install
COPY jepsen/jepsen /jepsen/jepsen/
RUN if [ -f /jepsen/jepsen/project.clj ]; then cd /jepsen/jepsen && lein install; fi
COPY jepsen /jepsen/

ADD ./bashrc /root/.bashrc
ADD ./init.sh /init.sh
RUN dos2unix /init.sh /root/.bashrc \
    && chmod +x /init.sh

CMD /init.sh
