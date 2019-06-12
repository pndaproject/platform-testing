FROM centos:7.6.1810

RUN yum install -y epel-release
RUN yum install -y python36-devel python36-pip gcc gcc-c++ cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel

COPY src/main/resources /platform-testing/

WORKDIR /platform-testing

RUN pip3 install --upgrade pip
RUN find . -type f -name 'requirements.txt' | xargs -t -L1 pip3 install -r
