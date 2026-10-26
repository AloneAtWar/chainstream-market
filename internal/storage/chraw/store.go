// Package chraw stores ch-loader's raw chain facts in ClickHouse.
package chraw

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"gorm.io/gorm"
)

// tables lists every fact table; each is deleted by block_height.
var tables = []string{"raw_transfers", "raw_trades", "oracle_updates", "asset_facts", "pool_facts"}

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
	var rows []checkpointRow
	err := store.db.WithContext(ctx).Select("kafka_offset, block_height, block_hash, updated_at").
		Where("chain_id = ?", chainID).Order("kafka_offset DESC").Limit(1).Find(&rows).Error
	if err != nil {
		return chainsync.Checkpoint{}, false, fmt.Errorf("clickhouse raw store: load checkpoint: %w", err)
	}
	if len(rows) == 0 {
		return chainsync.Checkpoint{}, false, nil
	}
	row := rows[0]
	return chainsync.Checkpoint{
		ChainID: chainID, Offset: row.KafkaOffset, Height: row.BlockHeight, Hash: row.BlockHash,
		UpdatedAt: row.UpdatedAt.UTC(),
	}, true, nil
}

func (store *Store) SaveRawCheckpoint(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	if checkpoint.ChainID == 0 {
		return errors.New("clickhouse raw store: checkpoint chain id is required")
	}
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	return store.insert(ctx, "raw_checkpoints", &[]checkpointRow{{
		ChainID: checkpoint.ChainID, KafkaOffset: checkpoint.Offset, BlockHeight: checkpoint.Height,
		BlockHash: checkpoint.Hash, UpdatedAt: updatedAt.UTC(),
	}})
}

func (store *Store) DeleteRawAbove(ctx context.Context, chainID uint64, height int64) error {
	for _, table := range tables {
		if err := store.db.WithContext(ctx).Exec("DELETE FROM "+table+" WHERE chain_id = ? AND block_height > ?",
			chainID, height).Error; err != nil {
			return fmt.Errorf("clickhouse raw store: delete %s above %d: %w", table, height, err)
		}
	}
	return nil
}

// WriteRaw sends one insert per table for all of blocks, skipping tables
// with no rows.
func (store *Store) WriteRaw(ctx context.Context, blocks []model.RawBlock) error {
	for _, block := range blocks {
		if block.ChainID == 0 {
			return errors.New("clickhouse raw store: block chain id is required")
		}
	}
	var (
		transfers []transferRow
		trades    []tradeRow
		oracles   []oracleRow
		assets    []assetRow
		pools     []poolRow
	)
	for _, block := range blocks {
		transfers = appendTransfers(transfers, block)
		trades = appendTrades(trades, block)
		oracles = appendOracles(oracles, block)
		assets = appendAssets(assets, block)
		pools = appendPools(pools, block)
	}
	writes := []struct {
		table string
		count int
		rows  any
	}{
		{"raw_transfers", len(transfers), &transfers},
		{"raw_trades", len(trades), &trades},
		{"oracle_updates", len(oracles), &oracles},
		{"asset_facts", len(assets), &assets},
		{"pool_facts", len(pools), &pools},
	}
	for _, write := range writes {
		if write.count == 0 {
			continue
		}
		if err := store.insert(ctx, write.table, write.rows); err != nil {
			return err
		}
	}
	return nil
}

func appendTransfers(rows []transferRow, block model.RawBlock) []transferRow {
	for _, transfer := range block.Transfers {
		rows = append(rows, transferRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: transfer.TransactionHash, TransactionIndex: transfer.TransactionIndex,
			LogIndex: transfer.LogIndex, Token: transfer.Token, FromAddress: transfer.From, ToAddress: transfer.To,
			Amount: transfer.Amount,
		})
	}
	return rows
}

func appendTrades(rows []tradeRow, block model.RawBlock) []tradeRow {
	for _, trade := range block.Trades {
		row := tradeRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: trade.TransactionHash, TransactionIndex: trade.TransactionIndex,
			LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, Protocol: trade.Protocol,
			DexAddress: trade.DexAddress, PoolID: trade.PoolID, Action: trade.Action, Sender: trade.Sender,
			Trader: trade.Trader, Recipient: trade.Recipient,
			AmountAssets: make([]string, len(trade.Amounts)), AmountRaw: make([]string, len(trade.Amounts)),
			AmountValues: make([]string, len(trade.Amounts)), AmountDecimals: make([]uint32, len(trade.Amounts)),
			AmountDecimalsKnown: make([]bool, len(trade.Amounts)), ReservesAfter: trade.ReservesAfter,
			SqrtPriceX96: trade.SqrtPriceX96, Liquidity: trade.Liquidity, Tick: trade.Tick, ActiveBinID: trade.ActiveBinID,
		}
		for index, amount := range trade.Amounts {
			row.AmountAssets[index], row.AmountRaw[index], row.AmountValues[index] = amount.Asset, amount.Raw, amount.Value
			row.AmountDecimals[index], row.AmountDecimalsKnown[index] = amount.Decimals, amount.DecimalsKnown
		}
		if row.ReservesAfter == nil {
			row.ReservesAfter = []string{}
		}
		rows = append(rows, row)
	}
	return rows
}

func appendOracles(rows []oracleRow, block model.RawBlock) []oracleRow {
	for _, update := range block.Oracles {
		rows = append(rows, oracleRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: update.TransactionHash, TransactionIndex: update.TransactionIndex,
			LogIndex: update.LogIndex, Feed: update.Feed, Answer: update.Answer, RoundID: update.RoundID,
			UpdatedAt: update.UpdatedAt,
		})
	}
	return rows
}

func appendAssets(rows []assetRow, block model.RawBlock) []assetRow {
	for _, fact := range block.Assets {
		rows = append(rows, assetRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime, Address: fact.Address,
			Standard: fact.Standard, Recognition: fact.Recognition, MetadataComplete: fact.MetadataComplete,
			Type: fact.Type, SourceHeight: fact.SourceHeight, DeploymentHeight: fact.DeploymentHeight,
			DeploymentTime: fact.DeploymentTime, DeploymentTxIndex: fact.DeploymentTxIdx,
			DeploymentTxHash: fact.DeploymentTxHash, DeploymentTraceID: fact.DeploymentTrace,
			MetadataJSON: fact.MetadataJSON,
		})
	}
	return rows
}

func appendPools(rows []poolRow, block model.RawBlock) []poolRow {
	for _, fact := range block.Pools {
		tokens := fact.TokenAddresses
		if tokens == nil {
			tokens = []string{}
		}
		rows = append(rows, poolRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime, PoolID: fact.PoolID,
			Protocol: fact.Protocol, DexAddress: fact.DexAddress, TokenAddresses: tokens,
			SourceHeight: fact.SourceHeight, SourceTime: fact.SourceTime, Created: fact.Created,
			MetadataJSON: fact.MetadataJSON,
		})
	}
	return rows
}

func (store *Store) insert(ctx context.Context, table string, rows any) error {
	if err := chdb.Insert(ctx, store.db, rows); err != nil {
		return fmt.Errorf("clickhouse raw store: insert %s: %w", table, err)
	}
	return nil
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
	var rows []transferRow
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

var (
	_ storage.RawStore       = (*Store)(nil)
	_ storage.TransferReader = (*Store)(nil)
)

// ApplyRetention sets the TTL of ch-loader's tables. Only ch-loader calls
// it, so market-api, which opens the store to read transfers, leaves the TTL
// alone.
func (store *Store) ApplyRetention(ctx context.Context, retention chdb.Retention) error {
	return chdb.ApplyTTL(ctx, store.db, retention, retention.RawTTL())
}
