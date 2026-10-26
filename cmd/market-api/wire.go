//go:build wireinject

package main

import (
	"context"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/app/api/data"
	"github.com/AloneAtWar/chainstream-market/internal/app/api/server"
	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/google/wire"
)

// wireApp initializes the Kratos application.
func wireApp(context.Context, *conf.Bootstrap, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(
		conf.ProviderSet,
		data.ProviderSet,
		service.ProviderSet,
		server.ProviderSet,
		newApp,
	))
}
