# API SwiftPay
.PHONY: run test integration db-up db-down migrate lint vet build

APP_NAME := swiftpay-api
BUILD_DIR := ./bin
MIGRATIONS_DIR := ./internal/store/postgres/migrations
DB_DSN := postgres://swiftpay:swiftpay@localhost:5432/swiftpay?sslmode=disable

run:
	go run ./cmd/server/

build:
	go build -ldflags="-s -w" -o $(BUILD_DIR)/$(APP_NAME) ./cmd/server/

test:
	go test ./internal/... -v -count=1

integration:
	go test -tags=integration ./test/integration/ -v -count=1

db-up:
	docker-compose up -d postgres

db-down:
	docker-compose down -v

migrate:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_DSN)" up

migrate-down:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_DSN)" down -all

lint:
	golangci-lint run ./...

vet:
	go vet ./...
