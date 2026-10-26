APP := market-engine
VERSION := $(shell git describe --tags --always --dirty)
GOLANGCI_LINT_VERSION := v2.12.2
WIRE_VERSION := v0.7.0

.PHONY: build clean config docker down generate logs up lint lint-install proto-check test test-race fmt fmt-check verify wire

build:
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/$(APP) ./cmd/market-engine
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/market-api ./cmd/market-api
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/holder-indexer ./cmd/holder-indexer
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/market-registry ./cmd/market-registry
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/market-rankings ./cmd/market-rankings
	go build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/ch-loader ./cmd/ch-loader

# The local stack in deploy/ runs chainstream-evm from its own checkout,
# by default next to this one.
EVM_DIR ?= ../chainstream-evm
COMPOSE := docker compose -f deploy/docker-compose.yml

docker:
	docker build --build-arg VERSION=$(VERSION) -t chainstream-market:latest .

up: docker
	$(MAKE) -C $(EVM_DIR) docker
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

config:
	buf generate --template buf.gen.config.yaml

wire:
	go run github.com/google/wire/cmd/wire@$(WIRE_VERSION) ./cmd/...

generate: config wire
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
	buf lint && buf build
	cd proto && buf lint && buf build

verify: proto-check fmt-check lint test-race
