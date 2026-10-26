# syntax=docker/dockerfile:1
# One image with every chainstream-market command; pick one as the command,
# e.g. ["market-engine", "-conf", "/etc/chainstream-market/config.yaml"].
FROM golang:1.26.10-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY . .
ARG VERSION=dev
# A vendor/ directory, from go mod vendor, builds without the network.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    if [ ! -d vendor ]; then go mod download; fi \
    && for command in market-engine market-api holder-indexer market-registry market-rankings ch-loader; do \
      go build -trimpath -ldflags "-X main.Version=${VERSION}" -o /out/$command ./cmd/$command || exit 1; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY configs /etc/chainstream-market
EXPOSE 8080 9100
