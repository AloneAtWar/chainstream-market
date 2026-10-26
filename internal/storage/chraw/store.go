// Package chraw reads ch-loader's raw chain facts from ClickHouse: the
// transfers market-api serves and the trades market-engine backfills from.
package chraw

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
)

// Store writes raw facts as plain inserts through GORM and deletes by
// height. It keeps no per-chain state, so one Store serves every chain.
type Store struct {
	db *gorm.DB
}

// Open connects, and creates the database and tables if needed.
func Open(ctx context.Context, options chdb.Options) (*Store, error) {
	db, err := chdb.Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	if err := chdb.Migrate(ctx, db, chdb.RawSchema); err != nil {
		return nil, errors.Join(fmt.Errorf("clickhouse raw store: migrate: %w", err), chdb.Close(db))
	}
	return &Store{db: db}, nil
}

func (store *Store) Close() error {
	return chdb.Close(store.db)
}

func (store *Store) LoadRawCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	return chdb.LoadRawCheckpoint(ctx, store.db, chainID)
}

// Transfers returns matching transfers newest first.
func (store *Store) Transfers(ctx context.Context, query model.TransferQuery) ([]model.Transfer, error) {
	if query.Limit <= 0 || (query.Token == "" && query.Holder == "") {
		return nil, errors.New("clickhouse raw store: transfer query needs a limit and a token or holder")
	}
	filters := []string{"chain_id = ?"}
	args := []any{query.ChainID}
	if query.Token != "" {
		filters = append(filters, "token = ?")
		args = append(args, query.Token)
	}
	if query.Holder != "" {
		// Matches the raw_transfers_party index expression exactly.
		filters = append(filters, "has([from_address, to_address], ?)")
		args = append(args, query.Holder)
	}
	switch {
	case query.BeforeHeight > 0 && query.BeforeLogIndex != nil:
		filters = append(filters, "(block_height < ? OR (block_height = ? AND log_index < ?))")
		args = append(args, query.BeforeHeight, query.BeforeHeight, *query.BeforeLogIndex)
	case query.BeforeHeight > 0:
		filters = append(filters, "block_height < ?")
		args = append(args, query.BeforeHeight)
	}
	var rows []chdb.TransferRow
	err := store.db.WithContext(ctx).Select(`block_height, block_time, transaction_hash, transaction_index,
		log_index, token, from_address, to_address, amount`).
		Where(strings.Join(filters, " AND "), args...).
		Order("block_height DESC, log_index DESC").Limit(query.Limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query transfers: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	transfers := make([]model.Transfer, len(rows))
	for index, row := range rows {
		transfers[index] = model.Transfer{BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.UTC(), RawTransfer: model.RawTransfer{
			TransactionHash: row.TransactionHash, TransactionIndex: row.TransactionIndex, LogIndex: row.LogIndex,
			Token: row.Token, From: row.FromAddress, To: row.ToAddress, Amount: row.Amount,
		}}
	}
	return transfers, nil
}

var _ storage.TransferReader = (*Store)(nil)
