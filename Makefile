APP := market-engine
VERSION := $(shell git describe --tags --always --dirty)
GOLANGCI_LINT_VERSION := v2.12.2

.PHONY: build generate lint lint-install proto-check test test-race fmt fmt-check verify

build:
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/$(APP) ./cmd/market-engine
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/market-api ./cmd/market-api

generate:
	cd proto && buf generate
	cd proto && go mod tidy
	go mod tidy

test:
	go test ./...
	cd proto && go build ./...

test-race:
	go test -race ./...

fmt:
	golangci-lint fmt

fmt-check:
	golangci-lint fmt --diff

lint:
	golangci-lint run ./...

lint-install:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

proto-check:
	cd proto && buf lint && buf build

verify: proto-check fmt-check lint test-race
