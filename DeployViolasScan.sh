#!/bin/bash

sudo docker stop violas-scanner
sudo docker rm violas-scanner
sudo docker image rm violas-scanner
sed -i "s/LibraChainScanner/ViolasChainScanner/g" Dockerfile
sudo docker image build --no-cache -t violas-scanner .
sudo docker run --name=violas-scanner --network=host -d violas-scanner
