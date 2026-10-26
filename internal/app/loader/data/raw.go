package data

import (
	"context"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/app/loader/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// rawRepo writes raw facts as plain inserts and deletes by height. It keeps
// no per-chain state, so one serves every chain.
type rawRepo struct {
	db *gorm.DB
}

// NewRawRepo returns ch-loader's repository.
func NewRawRepo(db *gorm.DB) biz.RawRepo { return &rawRepo{db: db} }

func (repo *rawRepo) LoadRawCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	return chdb.LoadRawCheckpoint(ctx, repo.db, chainID)
}

func (repo *rawRepo) SaveRawCheckpoint(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	return chdb.SaveRawCheckpoint(ctx, repo.db, checkpoint)
}

func (repo *rawRepo) WriteRaw(ctx context.Context, blocks []model.RawBlock) error {
	return chdb.WriteRaw(ctx, repo.db, blocks)
}

func (repo *rawRepo) DeleteRawAbove(ctx context.Context, chainID uint64, height int64) error {
	for _, table := range chdb.RawTables {
		if err := repo.db.WithContext(ctx).Exec("DELETE FROM "+table+" WHERE chain_id = ? AND block_height > ?",
			chainID, height).Error; err != nil {
			return fmt.Errorf("clickhouse raw store: delete %s above %d: %w", table, height, err)
		}
	}
	return nil
}
