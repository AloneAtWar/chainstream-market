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

// registryReader reads market-registry's tokens and pools.
type registryReader struct {
	db *gorm.DB
}

// NewRegistryReader reads market-registry's tokens and pools.
func NewRegistryReader(data *Data) service.RegistryReader { return &registryReader{db: data.state} }

func (reader *registryReader) Token(ctx context.Context, chainID uint64, address string) (model.Token, bool, error) {
	var row pgdb.TokenRow
	err := reader.db.WithContext(ctx).Select(pgdb.TokenColumns).
		Where("chain_id = ? AND address = ?", int64(chainID), address).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Token{}, false, nil
	}
	if err != nil {
		return model.Token{}, false, fmt.Errorf("postgres registry: token: %w", err)
	}
	token, err := row.Token()
	if err != nil {
		return model.Token{}, false, fmt.Errorf("postgres registry: read token: %w", err)
	}
	return token, true, nil
}

func (reader *registryReader) Pool(ctx context.Context, chainID uint64, poolID string) (model.Pool, bool, error) {
	var row pgdb.PoolRow
	err := reader.db.WithContext(ctx).Select(pgdb.PoolColumns).
		Where("chain_id = ? AND pool_id = ?", int64(chainID), poolID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Pool{}, false, nil
	}
	if err != nil {
		return model.Pool{}, false, fmt.Errorf("postgres registry: pool: %w", err)
	}
	return row.Pool(), true, nil
}

func (reader *registryReader) TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]model.Pool, error) {
	var rows []pgdb.PoolRow
	err := reader.db.WithContext(ctx).Select(pgdb.PoolColumns).
		Where("chain_id = ? AND tokens @> ?::text[]", int64(chainID), pgdb.Array([]string{token})).
		Order("source_height DESC, pool_id").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres registry: token pools: %w", err)
	}
	pools := make([]model.Pool, len(rows))
	for index, row := range rows {
		pools[index] = row.Pool()
	}
	return pools, nil
}
