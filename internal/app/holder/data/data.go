// Package data is holder-indexer's data layer: the chain topics it reads,
// the PostgreSQL tables it keeps balances in and the JSON-RPC client it
// reads balanceOf through.
package data

import (
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
var ProviderSet = wire.NewSet(pgdb.OpenConfig, source.NewSources, NewHolderRepo)
