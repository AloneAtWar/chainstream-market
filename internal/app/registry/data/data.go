// Package data is market-registry's data layer: the chain topics it reads
// and the PostgreSQL tables it keeps token and pool metadata in.
package data

import (
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(pgdb.OpenConfig, source.NewSources, NewRegistryRepo)
