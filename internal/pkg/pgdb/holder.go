package pgdb

import (
	"fmt"
	"math/big"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// HoldingRow is a holder_balances row read with balance as text.
// holder-indexer writes it and market-api reads it.
type HoldingRow struct {
	Token   string
	Holder  string
	Balance string
}

// Holdings decodes rows.
func Holdings(rows []HoldingRow) ([]model.Holding, error) {
	holdings := make([]model.Holding, len(rows))
	for index, row := range rows {
		value, ok := new(big.Int).SetString(row.Balance, 10)
		if !ok {
			return nil, fmt.Errorf("postgres store: read holdings: invalid balance %q", row.Balance)
		}
		holdings[index] = model.Holding{HolderKey: model.HolderKey{Token: row.Token, Holder: row.Holder}, Balance: value}
	}
	return holdings, nil
}

// HolderCountRow is a holder_counts row.
type HolderCountRow struct {
	ChainID int64 `gorm:"primaryKey;autoIncrement:false"`
	Token   string
	Holders int64
}

func (HolderCountRow) TableName() string { return "holder_counts" }
