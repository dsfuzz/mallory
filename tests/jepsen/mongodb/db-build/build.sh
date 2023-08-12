#!/bin/bash

# ###
# To build mongodb .deb packages
# ###

# Create container: mongodb-build
docker.io/jgoerzen/debian-base-standard:buster

docker container rm mongodb-build > /dev/null
docker run -it --name mongodb-build -d jgoerzen/debian-base-standard:buster bash

# Copy tutorials from host to container
docker cp ../../../../instrumentor mongodb-build:/opt
docker cp ./install.sh mongodb-build:/opt

# Build subjects
docker exec mongodb-build bash /opt/install.sh

# Copy packages from container to host
docker cp mongodb-build:/opt/packages .

# Stop container
docker stop mongodb-build
