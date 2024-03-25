FROM golang:1.21.8-bookworm as build-root

WORKDIR /build

COPY go.mod .
COPY go.sum .

COPY . .

ENV GOOS=linux
ENV GOARCH=amd64

RUN go build --buildvcs=false

FROM ubuntu:20.04

RUN apt-get update -y && apt-get install -y vim dnsutils
COPY --from=build-root /build/discoenv-users /usr/local/bin

ENTRYPOINT ["discoenv-users"]

EXPOSE 60000
