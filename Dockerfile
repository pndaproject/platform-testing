FROM centos:7.6.1810

RUN yum install -y python-devel epel-release
RUN yum install -y python2-pip gcc gcc-c++ cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel

COPY src/main/resources /platform-testing/

WORKDIR /platform-testing

RUN pip install --upgrade pip && pip install setuptools==28.8.0
RUN find . -type f -name 'requirements.txt' | xargs -t -L1 pip install -r
