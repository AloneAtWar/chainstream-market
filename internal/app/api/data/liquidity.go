package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// liquidityReader reads the tick tables market-engine keeps in its
// PostgreSQL state.
type liquidityReader struct {
	db *gorm.DB
}

// NewLiquidityReader reads market-engine's tick tables, which it stores when
// its state.driver is postgres.
func NewLiquidityReader(data *Data) service.LiquidityReader { return &liquidityReader{db: data.state} }

func (reader *liquidityReader) PoolTickTable(ctx context.Context, chainID uint64, poolID string) (model.PoolTickTable, bool, error) {
	var table model.PoolTickTable
	found := false
	err := reader.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var record struct{ Value string }
		err := tx.Raw(`SELECT value::text AS value FROM engine_state WHERE chain_id = ? AND kind = ? AND key = ?`,
			int64(chainID), model.StateKindCLAMMPools, poolID).Take(&record).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pool: %w", err)
		}
		if err := json.Unmarshal([]byte(record.Value), &table.Pool); err != nil {
			return fmt.Errorf("decode pool: %w", err)
		}
		var rows []struct {
			Tick int32
			Net  string
		}
		if err := tx.Raw(`SELECT split_part(key, '|', 2)::integer AS tick, value #>> '{}' AS net FROM engine_state
			WHERE chain_id = ? AND kind = ? AND split_part(key, '|', 1) = ?`,
			int64(chainID), model.StateKindTicks, poolID).Scan(&rows).Error; err != nil {
			return fmt.Errorf("ticks: %w", err)
		}
		table.Ticks = make(map[int]*big.Int, len(rows))
		for _, row := range rows {
			net, ok := new(big.Int).SetString(row.Net, 10)
			if !ok {
				return fmt.Errorf("tick %d has liquidity %q", row.Tick, row.Net)
			}
			table.Ticks[int(row.Tick)] = net
		}
		found = true
		return nil
	})
	if err != nil {
		return model.PoolTickTable{}, false, fmt.Errorf("postgres liquidity: %s: %w", poolID, err)
	}
	return table, found, nil
}
