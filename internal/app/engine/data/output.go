package data

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// tables lists every output table with the column Rewind deletes by. A trade
// or price recomputed after its token's decimals arrived keeps its own
// block_height and is deleted by the block that recomputed it.
var tables = []struct{ name, height string }{
	{"blocks", "block_height"},
	{"trades", "revision_height"},
	{"token_prices", "revision_height"},
	{"candles", "update_height"},
}

// outputRepo writes each block's output to ClickHouse as plain inserts
// through GORM. Rewriting a height that may already exist, after a restart
// or a reorg, deletes it first, so every height holds exactly one copy of
// its output.
type outputRepo struct {
	db *gorm.DB
	mu sync.Mutex
	// written is the highest height each chain may have rows at; blocks
	// above it need no delete before insert.
	written map[uint64]int64
}

// NewOutputRepo writes to db, which has the market tables, and loads the
// highest stored height per chain.
func NewOutputRepo(ctx context.Context, db *gorm.DB) (biz.Output, error) {
	return newOutputRepo(ctx, db)
}

func newOutputRepo(ctx context.Context, db *gorm.DB) (*outputRepo, error) {
	repo := &outputRepo{db: db, written: make(map[uint64]int64)}
	if err := repo.loadWritten(ctx); err != nil {
		return nil, err
	}
	return repo, nil
}

func (repo *outputRepo) loadWritten(ctx context.Context) error {
	var heights []struct {
		ChainID uint64
		Height  int64
	}
	if err := repo.db.WithContext(ctx).Model(&chdb.BlockRow{}).Select("chain_id, max(block_height) AS height").
		Group("chain_id").Find(&heights).Error; err != nil {
		return fmt.Errorf("clickhouse output: load heights: %w", err)
	}
	for _, row := range heights {
		repo.written[row.ChainID] = row.Height
	}
	return nil
}

func (repo *outputRepo) WriteBlock(ctx context.Context, block model.BlockOutput) error {
	if block.ChainID == 0 {
		return errors.New("clickhouse output: block chain id is required")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if written, ok := repo.written[block.ChainID]; ok && block.Height <= written {
		if err := repo.deleteFrom(ctx, block.ChainID, "=", block.Height); err != nil {
			return err
		}
	}
	if err := chdb.InsertBlock(ctx, repo.db, block); err != nil {
		return err
	}
	if block.Height > repo.written[block.ChainID] {
		repo.written[block.ChainID] = block.Height
	}
	return nil
}

func (repo *outputRepo) Rewind(ctx context.Context, chainID uint64, rollbackTo int64) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := repo.deleteFrom(ctx, chainID, ">", rollbackTo); err != nil {
		return err
	}
	if written, ok := repo.written[chainID]; ok && written > rollbackTo {
		repo.written[chainID] = rollbackTo
	}
	return nil
}

func (repo *outputRepo) deleteFrom(ctx context.Context, chainID uint64, operator string, height int64) error {
	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s WHERE chain_id = ? AND %s %s ?", table.name, table.height, operator)
		if err := repo.db.WithContext(ctx).Exec(query, chainID, height).Error; err != nil {
			return fmt.Errorf("clickhouse output: delete %s %s %d: %w", table.name, operator, height, err)
		}
	}
	return nil
}

// applyRetention sets the TTL of market-engine's tables. Only market-engine
// applies it, so readers of the tables leave the TTL alone.
func applyRetention(ctx context.Context, db *gorm.DB, retention chdb.Retention) error {
	return chdb.ApplyTTL(ctx, db, retention, retention.EngineTTL())
}
