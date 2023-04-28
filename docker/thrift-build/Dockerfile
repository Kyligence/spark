FROM ubuntu:18.04
USER root

RUN mv /etc/apt/sources.list /etc/apt/sourses.list.backup
COPY ./sources.list /etc/apt/sources.list

RUN groupadd -g 1000 --system jenkins && useradd --uid 1000 --system jenkins -g jenkins && usermod jenkins -G root
RUN DEBIAN_FRONTEND=noninteractive apt-get update -y && \
    apt-get install -y git openjdk-8-jdk curl maven bzip2 unzip wget \
    automake bison flex g++ libboost-all-dev libevent-dev libssl-dev libtool make pkg-config \
    && apt install libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://deb.nodesource.com/setup_8.x | bash - && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs && rm -rf /var/lib/apt/lists/*

ENV PROGUARD_VERSION=6.2.2
RUN cd /usr/share && wget --no-check-certificate https://sourceforge.net/projects/proguard/files/proguard/6.2/proguard${PROGUARD_VERSION}.zip && unzip proguard${PROGUARD_VERSION}.zip && rm proguard${PROGUARD_VERSION}.zip
ENV PROGUARD_HOME /usr/share/proguard${PROGUARD_VERSION}
RUN cd /usr/share && wget --no-check-certificate https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2 && tar -xvf phantomjs-2.1.1-linux-x86_64.tar.bz2 && rm phantomjs-2.1.1-linux-x86_64.tar.bz2
ENV PHANTOMJS_HOME /usr/share/phantomjs-2.1.1-linux-x86_64
RUN cd /usr/share && wget http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz && tar zxvf thrift-0.13.0.tar.gz && rm thrift-0.13.0.tar.gz
ENV THRIFT_VERSION=v0.13.0
RUN cd /usr/share/thrift-0.13.0 && chmod +x configure && ./bootstrap.sh && ./configure && make && make install

# RUN apt update && apt -y upgrade && apt install -y python3-pip
# RUN pip3 install thrift@0.13.0
# RUN npm install thrift@0.13.0 -g
# RUN thrift -version

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV MAVEN_HOME /usr/share/maven
ENV PATH $PHANTOMJS_HOME/bin:$PROGUARD_HOME/bin:$MAVEN_HOME/bin:$JAVA_HOME/bin:$PATH
ENV LANG C.UTF-8
RUN cp $PROGUARD_HOME/bin/proguard.sh $PROGUARD_HOME/bin/proguard
RUN cd /bin && rm -f sh && ln -s /bin/bash sh
ENTRYPOINT ["bash"]