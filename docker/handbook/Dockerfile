FROM ubuntu:18.04
LABEL MAINTAINER lex <liming.meng@kyligence.io>

RUN mv /etc/apt/sources.list /etc/apt/sourses.list.backup
COPY ./sources.list /etc/apt/sources.list
COPY ./nodesource_setup.sh /gitbook/nodesource_setup.sh

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    bash /gitbook/nodesource_setup.sh && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y calibre-bin nodejs
RUN npm config set registry=http://mvn.ops.kylincorp.com/repository/npm-public/ && \
    npm install -g gitbook-cli && \
    npm install -g markdown-link-check && \
    DEBIAN_FRONTEND=noninteractive apt-get clean

ENV BOOKDIR /gitbook
VOLUME $BOOKDIR
EXPOSE 4000

WORKDIR /gitbook