FROM gradle:8.8.0-jdk11-alpine AS builder
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --info --no-daemon -x test

COPY modules ./modules
RUN gradle installDist --no-daemon

###

FROM alpine:3.20.1 as application
RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ && apk add redis

WORKDIR /application

COPY --from=builder /home/gradle/src/build/install/depl-da-prototype .

ENTRYPOINT ["sh", "-c", "/application/bin/depl-da-prototype"]

###

FROM alpine:3.20.1 as tests
RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /tests

COPY --from=builder /home/gradle/src/modules/tests/build/install/tests .

ENTRYPOINT ["sh", "-c", "/tests/bin/tests"]
