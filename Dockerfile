FROM alpine:3.22.1

RUN apk add --no-cache openjdk11 jattach bash curl --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /application

ADD build/distributions/depl-da-prototype-0.1.0.tar /application

EXPOSE 8080

ENTRYPOINT ["/application/depl-da-prototype-0.1.0/bin/depl-da-prototype"]
