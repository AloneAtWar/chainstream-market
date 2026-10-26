# syntax=docker/dockerfile:1
# One image with every chainstream-market command; pick one as the command,
# e.g. ["market-engine", "-conf", "/etc/chainstream-market/config.yaml"].
FROM golang:1.26.10-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOPRIVATE=github.com/AloneAtWar GONOSUMDB=github.com/AloneAtWar
COPY . .
ARG VERSION=dev
# A vendor/ directory, from go mod vendor, builds without the network.
# Otherwise the private oh-my-blockchain module is fetched over SSH with the
# agent passed by docker build --ssh default.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build --mount=type=ssh \
    if [ ! -d vendor ]; then \
      mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null \
      && git config --global url."git@github.com:".insteadOf "https://github.com/" \
      && go mod download; \
    fi \
    && for command in market-engine market-api holder-indexer market-registry market-rankings ch-loader; do \
      go build -trimpath -ldflags "-X main.Version=${VERSION}" -o /out/$command ./cmd/$command || exit 1; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY configs /etc/chainstream-market
EXPOSE 8080 9100
