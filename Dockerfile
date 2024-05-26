FROM alpine:3.20.0

RUN apk add --no-cache openjdk11 jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /application

ADD build/distributions/depl-da-prototype-*.tar /application

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "/application/depl-da-prototype-*/bin/depl-da-prototype"]
