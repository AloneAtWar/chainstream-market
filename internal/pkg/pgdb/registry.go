package pgdb

import (
	"encoding/json"
	"fmt"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// TokenColumns selects a registry_tokens row into TokenRow.
const TokenColumns = `address, standard, recognition, metadata_complete, name, symbol, decimals,
	metadata::text AS metadata, created, deployment::text AS deployment, source_height, first_height, updated_height`

// TokenRow is a registry_tokens row read with its JSONB columns as text.
// market-registry writes it and market-api reads it.
type TokenRow struct {
	Address          string
	Standard         string
	Recognition      string
	MetadataComplete bool
	Name             *string
	Symbol           *string
	Decimals         *int16
	Metadata         *string
	Created          bool
	Deployment       *string
	SourceHeight     int64
	FirstHeight      int64
	UpdatedHeight    int64
}

func (TokenRow) TableName() string { return "registry_tokens" }

// Token decodes the row.
func (row TokenRow) Token() (model.Token, error) {
	token := model.Token{
		Address: row.Address, Standard: row.Standard, Recognition: row.Recognition,
		MetadataComplete: row.MetadataComplete, Name: row.Name, Symbol: row.Symbol, Created: row.Created,
		SourceHeight: row.SourceHeight, FirstHeight: row.FirstHeight, UpdatedHeight: row.UpdatedHeight,
	}
	if row.Decimals != nil {
		value := uint8(*row.Decimals)
		token.Decimals = &value
	}
	if row.Metadata != nil {
		token.Metadata = []byte(*row.Metadata)
	}
	if row.Deployment != nil {
		token.Deployment = &model.Deployment{}
		if err := json.Unmarshal([]byte(*row.Deployment), token.Deployment); err != nil {
			return token, fmt.Errorf("deployment of %s: %w", token.Address, err)
		}
	}
	return token, nil
}

// PoolColumns selects a registry_pools row into PoolRow.
const PoolColumns = `pool_id, protocol, dex_address, tokens, created, source_height, source_time,
	metadata::text AS metadata, first_height, updated_height`

// PoolRow is a registry_pools row read with metadata as text. market-registry
// writes it and market-api reads it.
type PoolRow struct {
	PoolID        string
	Protocol      string
	DexAddress    string
	Tokens        StringArray
	Created       bool
	SourceHeight  int64
	SourceTime    int64
	Metadata      *string
	FirstHeight   int64
	UpdatedHeight int64
}

func (PoolRow) TableName() string { return "registry_pools" }

// Pool decodes the row.
func (row PoolRow) Pool() model.Pool {
	pool := model.Pool{
		PoolID: row.PoolID, Protocol: row.Protocol, DexAddress: row.DexAddress, Tokens: []string(row.Tokens),
		Created: row.Created, SourceHeight: row.SourceHeight, SourceTime: row.SourceTime,
		FirstHeight: row.FirstHeight, UpdatedHeight: row.UpdatedHeight,
	}
	if row.Metadata != nil {
		pool.Metadata = []byte(*row.Metadata)
	}
	return pool
}
