#!/bin/bash

sudo docker stop libra-scanner
sudo docker rm libra-scanner
sudo docker image rm libra-scanner
sed -i "s/ViolasChainScanner/LibraChainScanner/g" Dockerfile
sudo docker image build --no-cache -t libra-scanner .
sudo docker run --name=libra-scanner --network=host -d libra-scanner
