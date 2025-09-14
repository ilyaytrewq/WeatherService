# build
FROM golang:1.22 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN go build -ldflags="-s -w" -o /smtp_service

# runtime
FROM alpine:3.18
RUN apk add --no-cache ca-certificates

RUN addgroup -S smtp && adduser -S -G smtp smtp

COPY --from=builder /smtp_service /usr/local/bin/smtp_service

USER smtp
WORKDIR /home/smtp

ENTRYPOINT ["/usr/local/bin/smtp_service"]
