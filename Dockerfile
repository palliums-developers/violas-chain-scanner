FROM ubuntu

RUN apt-get update
RUN apt-get -y upgrade

RUN apt-get -y install git python3 python3-pip

RUN git clone -b v0.18.3 https://Xing-Huang:13583744689edc@github.com/palliums-developers/libra-client.git
RUN pip3 install -r /libra-client/requirements.txt

COPY . /explorer-core
RUN pip3 install -r /explorer-core/requirements.txt

WORKDIR /explorer-core
RUN cp -rf ../libra-client/libra_client .
RUN cp -rf ../libra-client/violas_client .

CMD ["python3", "LibraExplorerCore.py"]
