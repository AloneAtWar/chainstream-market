//go:build wireinject

package main

import (
	"context"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/registry/conf"
	"github.com/AloneAtWar/chainstream-market/internal/app/registry/data"
	"github.com/AloneAtWar/chainstream-market/internal/app/registry/server"
	"github.com/go-kratos/kratos/v2"
	"github.com/google/wire"
)

// wireApp initializes the Kratos application.
func wireApp(context.Context, *conf.Bootstrap, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(
		conf.ProviderSet,
		data.ProviderSet,
		server.ProviderSet,
		newApp,
	))
}
