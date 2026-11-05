package data

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// launchpadReader reads market-launchpad's launchpad tokens.
type launchpadReader struct {
	db *gorm.DB
}

// NewLaunchpadReader reads market-launchpad's launchpad tokens.
//
// NewLaunchpadReader 读取 market-launchpad 维护的发射平台代币（三栏列表与单币详情）。
func NewLaunchpadReader(data *Data) service.LaunchpadReader { return &launchpadReader{db: data.state} }

// LaunchpadList reads one launchpad list.
func (reader *launchpadReader) LaunchpadList(ctx context.Context, query model.LaunchpadQuery) ([]model.LaunchpadListing, error) {
	return pgdb.LaunchpadList(ctx, reader.db, query)
}

// LaunchpadToken reads one launchpad token.
func (reader *launchpadReader) LaunchpadToken(
	ctx context.Context, chainID uint64, token string, targets model.LaunchpadTargets,
) (model.LaunchpadListing, bool, error) {
	return pgdb.LaunchpadToken(ctx, reader.db, chainID, token, targets)
}

// LaunchpadProtocols lists a chain's launchpad platforms.
func (reader *launchpadReader) LaunchpadProtocols(ctx context.Context, chainID uint64) ([]string, error) {
	return pgdb.LaunchpadProtocols(ctx, reader.db, chainID)
}
