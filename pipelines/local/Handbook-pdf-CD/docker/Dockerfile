FROM ubuntu:18.04
LABEL MAINTAINER lex <liming.meng@kyligence.io>

RUN mv /etc/apt/sources.list /etc/apt/sourses.list.backup
COPY ./sources.list /etc/apt/sources.list
COPY ./nodesource_setup.sh /book/nodesource_setup.sh

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    bash /book/nodesource_setup.sh && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends calibre-bin nodejs && \
    npm config set registry=http://mvn.ops.kylincorp.com/repository/npm-public/ && \
    npm install -g gitbook-cli && \
    DEBIAN_FRONTEND=noninteractive apt-get clean && rm -rf /var/lib/apt/lists/* && \
    cd /tmp && gitbook init && rm -f /tmp/*.md

WORKDIR /book