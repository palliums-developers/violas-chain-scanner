#!/bin/bash

sudo docker stop violas-scan
sudo docker rm violas-scan
sudo docker image rm violas-scan
sed -i "s/LibraExplorerCore/ViolasExplorerCore/g" Dockerfile
sudo docker image build --no-cache -t violas-scan .
sudo docker run --name=violas-scan --network=host -d violas-scan
