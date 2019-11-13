FROM python:3.7.3-alpine3.9 as base

RUN apk add --no-cache netcat-openbsd cyrus-sasl-dev cyrus-sasl-plain cyrus-sasl-gssapiv2

FROM base as builder

RUN apk add  build-base

COPY src/main/resources /platform-testing/

WORKDIR /platform-testing

RUN pip3 install --upgrade pip
RUN find . -type f -name 'requirements.txt' | xargs -t -n 1 pip3 install -r

FROM base

COPY --from=builder /platform-testing /platform-testing
COPY --from=builder /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages

WORKDIR /platform-testing
