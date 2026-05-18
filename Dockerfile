# API SwiftPay
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/swiftpay-api ./cmd/server/

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

ENV TZ=America/Sao_Paulo

COPY --from=builder /app/swiftpay-api /usr/local/bin/swiftpay-api

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/swiftpay-api"]
