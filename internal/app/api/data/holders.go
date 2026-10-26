package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// holderReader reads holder-indexer's balances.
type holderReader struct {
	db *gorm.DB
}

// NewHolderReader reads holder-indexer's balances.
func NewHolderReader(data *Data) service.HolderReader { return &holderReader{db: data.state} }

func (reader *holderReader) TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]model.Holding, error) {
	var rows []pgdb.HoldingRow
	err := reader.db.WithContext(ctx).Table("holder_balances AS b").Select("token, holder, balance::text AS balance").
		Where("chain_id = ? AND token = ? AND b.balance > 0", int64(chainID), token).
		Order("b.balance DESC, holder").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres holders: top holders: %w", err)
	}
	return pgdb.Holdings(rows)
}

func (reader *holderReader) HolderCount(ctx context.Context, chainID uint64, token string) (int64, error) {
	var row pgdb.HolderCountRow
	err := reader.db.WithContext(ctx).Where("chain_id = ? AND token = ?", int64(chainID), token).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("postgres holders: holder count: %w", err)
	}
	return row.Holders, nil
}
