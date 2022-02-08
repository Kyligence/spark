# Use an official Node.js runtime as a parent image
FROM node:8.11.1
# Set npm registry to China Taobao and install Gitbook
RUN npm config set registry https://registry.npm.taobao.org && \
    npm install gitbook-cli -g && \
    gitbook -V && \
    mkdir /gitbook
WORKDIR /gitbook
COPY . /gitbook
# Make port 4000 available to the world outside this container
EXPOSE 4000

CMD ["sh", "-c", "gitbook install /gitbook; gitbook serve /gitbook"]