#!/bin/sh

: "${SSH_PRIVATE_KEY?SSH_PRIVATE_KEY is empty, please use up.sh}"
: "${SSH_PUBLIC_KEY?SSH_PUBLIC_KEY is empty, please use up.sh}"

if [ ! -f ~/.ssh/known_hosts ]; then
    mkdir -m 700 ~/.ssh
    echo $SSH_PRIVATE_KEY | perl -p -e 's/↩/\n/g' > ~/.ssh/id_rsa
    chmod 600 ~/.ssh/id_rsa
    echo $SSH_PUBLIC_KEY > ~/.ssh/id_rsa.pub
    echo > ~/.ssh/known_hosts
    # wait a little bit for the last node to be up
    sleep 5
    # get nodes list
    sort -V /var/jepsen/shared/nodes > ~/nodes
    # Scan SSH keys
    while read node; do
      ssh-keyscan -t rsa,ecdsa,ed25519 $node >> ~/.ssh/known_hosts
    done <~/nodes
fi

# TODO: assert that SSH_PRIVATE_KEY==~/.ssh/id_rsa

cat <<EOF
Welcome to Jepsen on Docker
===========================

Please run \`bin/console\` in another terminal to proceed.
EOF

# Don't forward packets via the kernel; only the Click router can forward
# iptables -P FORWARD DROP
# Make sure non-experiment packets (including to the Internet) go via the sidenet
# ip route delete default
# ip route add default via 172.16.0.1

# Configure and start DNS server
# We overwrite the Docker-provided /etc/resolv.conf such that external hosts are resolved
# (If we use the default 127.0.1.11, querying for external hosts gets us in an infinite loop,
# where Docker queries dnsmasq, which queries Docker, and so on repeatedly.)
printf "server=8.8.8.8\nserver=8.8.4.4\nserver=1.1.1.1\n" > /etc/dnsmasq.conf
service dnsmasq start

# hack for keep this container running
tail -f /dev/null
