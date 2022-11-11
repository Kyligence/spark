FROM ubuntu:18.04
LABEL MAINTAINER lex <liming.meng@kyligence.io>

RUN mv /etc/apt/sources.list /etc/apt/sourses.list.backup
COPY ./sources.list /etc/apt/sources.list
COPY ./nodesource_setup.sh /gitbook/nodesource_setup.sh
COPY ./ghostpdl.tar.gz /gitbook/ghostpdl.tar.gz

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    bash /gitbook/nodesource_setup.sh && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y calibre-bin nodejs openssh-client build-essential autoconf make && \
    npm install -g gitbook-cli && \
    npm install -g markdown-link-check && \
    rm -rf /var/lib/apt/lists/*

RUN cd /gitbook && tar -zxvf ghostpdl.tar.gz && \
    cd ghostpdl && \
    ./autogen.sh && \
    make && \
    make install && \
    rm -rf ../ghostpdl*

ENV BOOKDIR /gitbook
VOLUME $BOOKDIR
EXPOSE 4000

WORKDIR /gitbook




