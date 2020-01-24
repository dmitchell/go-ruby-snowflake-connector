FROM ruby:2.3-slim-stretch as geminstall

ENV GOFILE=go1.13.6.linux-amd64.tar.gz
ENV GOFILE_SHA256=a1bc06deb070155c4f67c579f896a45eeda5a8fa54f35ba233304074c4abbbbd

# when this is run you must mount the code directory into /src

RUN apt update && apt install -y \
        build-essential \
        curl \
        git \
    && curl -O https://dl.google.com/go/${GOFILE}
    &&  echo "${GOFILE_SHA256}  ${GOFILE}" | sha256sum --check \
    && tar xzf ${GOFILE} \
    && mv go /usr/local

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

WORKDIR /src
