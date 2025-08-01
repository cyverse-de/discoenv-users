FROM golang:1.22 as build-root

WORKDIR /build

COPY go.mod .
COPY go.sum .

COPY . .

ENV GOOS=linux
ENV GOARCH=amd64

RUN go build --buildvcs=false

FROM debian:stable-slim

COPY --from=build-root /build/discoenv-users /usr/local/bin

ENTRYPOINT ["discoenv-users"]

EXPOSE 60000
