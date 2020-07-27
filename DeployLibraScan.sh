#!/bin/bash

sudo docker stop libra-scan
sudo docker rm libra-scan
sudo docker image rm libra-scan
sed -i "s/ViolasExplorerCore/LibraExplorerCore/g" Dockerfile
sudo docker image build -t libra-scan .
sudo docker run --name=libra-scan --rm --network=host -dit libra-scan python3 CreateLibraTable.py
sudo docker stop libra-scan
sudo docker run --name=libra-scan --network=host -d libra-scan
