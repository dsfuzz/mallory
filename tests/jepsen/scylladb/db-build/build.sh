#!/bin/bash

# ###
# To build scylladb .deb packages
# ###

# Create container: scylla-build
docker pull docker.io/scylladb/scylla-toolchain:fedora-34-20210902

docker container rm scylla-build > /dev/null
docker run -it --name scylla-build -d scylladb/scylla-toolchain:fedora-34-20210902 bash

# Copy tutorials from host to container
docker cp ../../../../instrumentor scylla-build:/opt
docker cp ./install.sh scylla-build:/opt

# Build subjects
docker exec scylla-build bash /opt/install.sh

# Copy packages from container to host
docker cp scylla-build:/opt/packages .

# Stop container
docker stop scylla-build
