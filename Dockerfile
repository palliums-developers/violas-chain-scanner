FROM ubuntu

RUN apt-get update
RUN apt-get -y upgrade

RUN apt-get -y install git python3 python3-pip

RUN git clone -b v0.30 https://Xing-Huang:13583744689edc@github.com/palliums-developers/violas-client.git libra-client
RUN git clone -b v0.30 https://Xing-Huang:13583744689edc@github.com/palliums-developers/violas-client.git violas-client
RUN pip3 install -r /libra-client/requirements.txt

COPY . /violas-chain-scanner
RUN pip3 install -r /violas-chain-scanner/requirements.txt

WORKDIR /violas-chain-scanner
RUN cp -rf ../libra-client/libra_client .
RUN cp -rf ../violas-client/violas_client .

CMD ["python3", "LibraChainScanner.py"]
