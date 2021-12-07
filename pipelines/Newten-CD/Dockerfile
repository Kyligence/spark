FROM ubuntu:21.10
USER root
RUN bash
RUN groupadd -g 1000 --system jenkins && useradd --uid 1000 --system jenkins -g jenkins && usermod jenkins -G root
RUN apt-get update -y && apt-get install -y \
    git \
    openjdk-8-jdk \
    curl \
    maven \
    bzip2 \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://deb.nodesource.com/setup_14.x | bash - && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs && rm -rf /var/lib/apt/lists/*
ENV PROGUARD_VERSION=6.2.2
RUN cd /usr/share && wget --no-check-certificate https://sourceforge.net/projects/proguard/files/proguard/6.2/proguard${PROGUARD_VERSION}.zip && unzip proguard${PROGUARD_VERSION}.zip && rm proguard${PROGUARD_VERSION}.zip
ENV PROGUARD_HOME /usr/share/proguard${PROGUARD_VERSION}
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV MAVEN_HOME /usr/share/maven
ENV PATH $PROGUARD_HOME/bin:$MAVEN_HOME/bin:$JAVA_HOME/bin:$PATH
ENV LANG C.UTF-8
RUN cp $PROGUARD_HOME/bin/proguard.sh $PROGUARD_HOME/bin/proguard
RUN cd /bin && rm -f sh && ln -s /bin/bash sh
ENTRYPOINT ["bash"]