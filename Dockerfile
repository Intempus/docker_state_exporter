FROM golang:1.24.6-alpine AS builder

# Ignore version pinning for build tools as they change frequently
# hadolint ignore=DL3018
RUN apk update --no-cache && apk add --no-cache \
    git \
    ca-certificates

COPY *.go go.mod go.sum $GOPATH/src/docker_state_exporter/

WORKDIR $GOPATH/src/docker_state_exporter/

RUN go mod vendor -v && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o /go/bin/docker_state_exporter

FROM alpine:3.21

RUN apk upgrade --no-cache

COPY --from=builder /go/bin/docker_state_exporter /go/bin/docker_state_exporter

EXPOSE 8080

ENTRYPOINT ["/go/bin/docker_state_exporter"]
CMD ["-listen-address=:8080"]
