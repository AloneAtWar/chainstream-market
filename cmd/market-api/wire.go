//go:build wireinject

package main

import (
	"context"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/data"
	"github.com/AloneAtWar/chainstream-market/internal/server"
	"github.com/AloneAtWar/chainstream-market/internal/service"
	"github.com/go-kratos/kratos/v2"
	"github.com/google/wire"
)

// wireApp initializes the Kratos application.
func wireApp(context.Context, *conf.Bootstrap, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(
		conf.ProviderSet,
		data.ProviderSet,
		service.ProviderSet,
		server.APISet,
		newApp,
	))
}
