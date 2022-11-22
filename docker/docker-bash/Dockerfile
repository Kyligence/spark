FROM docker:latest

MAINTAINER lp

RUN echo "https://mirrors.aliyun.com/alpine/v3.6/main/" > /etc/apk/repositories

RUN apk update \
        && apk upgrade \
        && apk add --no-cache bash \
        bash-doc \
        bash-completion \
        && rm -rf /var/cache/apk/* \
        && /bin/bash