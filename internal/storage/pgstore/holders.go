package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
)

func (store *Store) TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]model.Holding, error) {
	var rows []pgdb.HoldingRow
	err := store.db.WithContext(ctx).Table("holder_balances AS b").Select("token, holder, balance::text AS balance").
		Where("chain_id = ? AND token = ? AND b.balance > 0", int64(chainID), token).
		Order("b.balance DESC, holder").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: top holders: %w", err)
	}
	return pgdb.Holdings(rows)
}

func (store *Store) HolderCount(ctx context.Context, chainID uint64, token string) (int64, error) {
	var row pgdb.HolderCountRow
	err := store.db.WithContext(ctx).Where("chain_id = ? AND token = ?", int64(chainID), token).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("postgres store: holder count: %w", err)
	}
	return row.Holders, nil
}

var _ storage.HolderReader = (*Store)(nil)
