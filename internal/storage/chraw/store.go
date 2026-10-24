// Package chraw stores ch-loader's raw chain facts in ClickHouse.
package chraw

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// tables lists every fact table; each is deleted by block_height.
var tables = []string{"raw_transfers", "raw_trades", "oracle_updates", "asset_facts", "pool_facts"}

// Store writes raw facts as plain inserts and deletes by height. It keeps no
// per-chain state, so one Store serves every chain.
type Store struct {
	conn driver.Conn
}

// Open connects, and creates the database and tables if needed.
func Open(ctx context.Context, options chstore.Options) (*Store, error) {
	conn, err := chstore.Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	store := &Store{conn: conn}
	for _, statement := range schema {
		if err := conn.Exec(ctx, statement); err != nil {
			return nil, errors.Join(fmt.Errorf("clickhouse raw store: migrate: %w", err), conn.Close())
		}
	}
	return store, nil
}

func (store *Store) Close() error {
	return store.conn.Close()
}

func (store *Store) LoadRawCheckpoint(ctx context.Context, chainID uint64) (storage.Checkpoint, bool, error) {
	rows, err := store.conn.Query(ctx, `
		SELECT kafka_offset, block_height, block_hash, updated_at
		FROM raw_checkpoints WHERE chain_id = ?
		ORDER BY kafka_offset DESC LIMIT 1`, chainID)
	if err != nil {
		return storage.Checkpoint{}, false, fmt.Errorf("clickhouse raw store: load checkpoint: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	if !rows.Next() {
		return storage.Checkpoint{}, false, rows.Err()
	}
	checkpoint := storage.Checkpoint{ChainID: chainID}
	if err := rows.Scan(&checkpoint.Offset, &checkpoint.Height, &checkpoint.Hash, &checkpoint.UpdatedAt); err != nil {
		return storage.Checkpoint{}, false, fmt.Errorf("clickhouse raw store: scan checkpoint: %w", err)
	}
	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	return checkpoint, true, nil
}

func (store *Store) SaveRawCheckpoint(ctx context.Context, checkpoint storage.Checkpoint) error {
	if checkpoint.ChainID == 0 {
		return errors.New("clickhouse raw store: checkpoint chain id is required")
	}
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	return store.insert(ctx, "raw_checkpoints", func(batch driver.Batch) error {
		return batch.Append(checkpoint.ChainID, checkpoint.Offset, checkpoint.Height, checkpoint.Hash, updatedAt.UTC())
	})
}

func (store *Store) DeleteRawAbove(ctx context.Context, chainID uint64, height int64) error {
	for _, table := range tables {
		if err := store.conn.Exec(ctx, "DELETE FROM "+table+" WHERE chain_id = ? AND block_height > ?", chainID, height); err != nil {
			return fmt.Errorf("clickhouse raw store: delete %s above %d: %w", table, height, err)
		}
	}
	return nil
}

// WriteRaw sends one insert per table for all of blocks, skipping tables
// with no rows.
func (store *Store) WriteRaw(ctx context.Context, blocks []storage.RawBlock) error {
	for _, block := range blocks {
		if block.ChainID == 0 {
			return errors.New("clickhouse raw store: block chain id is required")
		}
	}
	writers := []struct {
		table string
		rows  func(storage.RawBlock) int
		fill  func(driver.Batch, storage.RawBlock) error
	}{
		{"raw_transfers", func(block storage.RawBlock) int { return len(block.Transfers) }, appendTransfers},
		{"raw_trades", func(block storage.RawBlock) int { return len(block.Trades) }, appendTrades},
		{"oracle_updates", func(block storage.RawBlock) int { return len(block.Oracles) }, appendOracles},
		{"asset_facts", func(block storage.RawBlock) int { return len(block.Assets) }, appendAssets},
		{"pool_facts", func(block storage.RawBlock) int { return len(block.Pools) }, appendPools},
	}
	for _, writer := range writers {
		total := 0
		for _, block := range blocks {
			total += writer.rows(block)
		}
		if total == 0 {
			continue
		}
		if err := store.insert(ctx, writer.table, func(batch driver.Batch) error {
			for _, block := range blocks {
				if err := writer.fill(batch, block); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func appendTransfers(batch driver.Batch, block storage.RawBlock) error {
	for _, transfer := range block.Transfers {
		if err := batch.Append(
			block.ChainID, block.Height, block.BlockTime, transfer.TransactionHash, transfer.TransactionIndex,
			transfer.LogIndex, transfer.Token, transfer.From, transfer.To, transfer.Amount,
		); err != nil {
			return err
		}
	}
	return nil
}

func appendTrades(batch driver.Batch, block storage.RawBlock) error {
	for _, trade := range block.Trades {
		assets := make([]string, len(trade.Amounts))
		raws := make([]string, len(trade.Amounts))
		values := make([]string, len(trade.Amounts))
		decimals := make([]uint32, len(trade.Amounts))
		known := make([]bool, len(trade.Amounts))
		for index, amount := range trade.Amounts {
			assets[index], raws[index], values[index] = amount.Asset, amount.Raw, amount.Value
			decimals[index], known[index] = amount.Decimals, amount.DecimalsKnown
		}
		reserves := trade.ReservesAfter
		if reserves == nil {
			reserves = []string{}
		}
		if err := batch.Append(
			block.ChainID, block.Height, block.BlockTime, trade.TransactionHash, trade.TransactionIndex,
			trade.LogIndex, trade.OperationIndex, trade.Protocol, trade.DexAddress, trade.PoolID, trade.Action,
			trade.Sender, trade.Trader, trade.Recipient, assets, raws, values, decimals, known,
			reserves, trade.SqrtPriceX96, trade.Liquidity, trade.Tick, trade.ActiveBinID,
		); err != nil {
			return err
		}
	}
	return nil
}

func appendOracles(batch driver.Batch, block storage.RawBlock) error {
	for _, update := range block.Oracles {
		if err := batch.Append(
			block.ChainID, block.Height, block.BlockTime, update.TransactionHash, update.TransactionIndex,
			update.LogIndex, update.Feed, update.Answer, update.RoundID, update.UpdatedAt,
		); err != nil {
			return err
		}
	}
	return nil
}

func appendAssets(batch driver.Batch, block storage.RawBlock) error {
	for _, fact := range block.Assets {
		if err := batch.Append(
			block.ChainID, block.Height, block.BlockTime, fact.Address, fact.Standard, fact.Recognition,
			fact.MetadataComplete, fact.Type, fact.SourceHeight, fact.DeploymentHeight, fact.DeploymentTime,
			fact.DeploymentTxIdx, fact.DeploymentTxHash, fact.DeploymentTrace, fact.MetadataJSON,
		); err != nil {
			return err
		}
	}
	return nil
}

func appendPools(batch driver.Batch, block storage.RawBlock) error {
	for _, fact := range block.Pools {
		tokens := fact.TokenAddresses
		if tokens == nil {
			tokens = []string{}
		}
		if err := batch.Append(
			block.ChainID, block.Height, block.BlockTime, fact.PoolID, fact.Protocol, fact.DexAddress,
			tokens, fact.SourceHeight, fact.SourceTime, fact.Created, fact.MetadataJSON,
		); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) insert(ctx context.Context, table string, fill func(driver.Batch) error) error {
	batch, err := store.conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return fmt.Errorf("clickhouse raw store: prepare %s: %w", table, err)
	}
	if err := fill(batch); err != nil {
		return errors.Join(fmt.Errorf("clickhouse raw store: append %s: %w", table, err), batch.Abort())
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse raw store: send %s: %w", table, err)
	}
	return nil
}

// Transfers returns matching transfers newest first.
func (store *Store) Transfers(ctx context.Context, query storage.TransferQuery) ([]storage.Transfer, error) {
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
	args = append(args, uint64(query.Limit))
	rows, err := store.conn.Query(ctx, `
		SELECT block_height, block_time, transaction_hash, transaction_index, log_index,
			token, from_address, to_address, amount
		FROM raw_transfers
		WHERE `+strings.Join(filters, " AND ")+`
		ORDER BY block_height DESC, log_index DESC
		LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse raw store: query transfers: %w", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var transfers []storage.Transfer
	for rows.Next() {
		var transfer storage.Transfer
		if err := rows.Scan(&transfer.BlockHeight, &transfer.BlockTime, &transfer.TransactionHash,
			&transfer.TransactionIndex, &transfer.LogIndex, &transfer.Token, &transfer.From, &transfer.To,
			&transfer.Amount); err != nil {
			return nil, fmt.Errorf("clickhouse raw store: scan transfer: %w", err)
		}
		transfer.BlockTime = transfer.BlockTime.UTC()
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse raw store: read transfers: %w", err)
	}
	return transfers, nil
}

var (
	_ storage.RawStore       = (*Store)(nil)
	_ storage.TransferReader = (*Store)(nil)
)
