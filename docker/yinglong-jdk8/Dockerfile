FROM openjdk:8-jdk-alpine as build
COPY &&replace_package&& /opt/

RUN mkdir -p /opt/kyligence \
    && mkdir -p /opt/kyligence/hadoop_conf \
    && tar -xf /opt/&&replace_dir&&*.tar.gz --strip-components 1 -C /opt/kyligence

FROM openjdk:8-jdk-alpine
RUN mkdir -p /opt/kyligence
COPY --from=build /opt/kyligence /opt/kyligence
RUN echo "https://mirror.tuna.tsinghua.edu.cn/alpine/v3.4/main/" > /etc/apk/repositories

ENV TZ Asia/Shanghai
ENV KYLIN_HOME /opt/kyligence
RUN apk update \
    && apk upgrade \
    && apk add --no-cache bash bash-doc bash-completion \
    && apk add tzdata && cp /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && apk del tzdata \
    && rm -rf /var/cache/apk/* \
    && cd /bin && rm -f sh && ln -s /bin/bash sh

WORKDIR ${KYLIN_HOME}
EXPOSE 7070

ENTRYPOINT ["bash", "/opt/kyligence/bin/kylin.sh", "start"]