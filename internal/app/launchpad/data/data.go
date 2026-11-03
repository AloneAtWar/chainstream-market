// Package data is market-launchpad's data layer: the chain topics it reads
// and the PostgreSQL tables it keeps launchpad tokens in.
//
// market-launchpad 的数据层：读取的链 topic 和保存发射平台代币的 PostgreSQL 表。
package data

import (
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/google/wire"
)

// ProviderSet is the data-layer provider set.
//
// ProviderSet 是数据层的 provider 集合。
var ProviderSet = wire.NewSet(pgdb.OpenConfig, source.NewSources, NewLaunchpadRepo)
