package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// walletRepo 把账本存在 PostgreSQL（持仓、undo、检查点）和 ClickHouse（流水）里。
type walletRepo struct {
	market *gorm.DB
	state  *gorm.DB
}

// NewWalletRepo 返回 market-wallet 的账本仓储。
func NewWalletRepo(data *Data) biz.WalletRepo {
	return &walletRepo{market: data.market, state: data.state}
}

// checkpointRow 是 wallet_checkpoints 的一行。
type checkpointRow struct {
	ChainID       int64 `gorm:"primaryKey;autoIncrement:false"`
	KafkaOffset   int64
	BlockHeight   int64
	Part          int32
	Parts         int32
	PrunedThrough int64
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
}

func (checkpointRow) TableName() string { return "wallet_checkpoints" }

func (repo *walletRepo) LoadWalletCheckpoint(ctx context.Context, chainID uint64) (biz.Checkpoint, bool, error) {
	var row checkpointRow
	err := repo.state.WithContext(ctx).Where("chain_id = ?", int64(chainID)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return biz.Checkpoint{}, false, nil
	}
	if err != nil {
		return biz.Checkpoint{}, false, fmt.Errorf("postgres store: load wallet checkpoint: %w", err)
	}
	return biz.Checkpoint{
		ChainID: chainID, Offset: row.KafkaOffset, Height: row.BlockHeight, Part: row.Part, Parts: row.Parts,
		PrunedThrough: row.PrunedThrough, UpdatedAt: row.UpdatedAt.UTC(),
	}, true, nil
}

func saveCheckpoint(tx *gorm.DB, checkpoint biz.Checkpoint) error {
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	row := checkpointRow{
		ChainID: int64(checkpoint.ChainID), KafkaOffset: checkpoint.Offset, BlockHeight: checkpoint.Height,
		Part: checkpoint.Part, Parts: checkpoint.Parts, PrunedThrough: checkpoint.PrunedThrough, UpdatedAt: updatedAt,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"kafka_offset", "block_height", "part", "parts", "pruned_through", "updated_at"}),
	}).Create(&row).Error
}

func (repo *walletRepo) DiscardTradesAfter(ctx context.Context, chainID uint64, height int64, part int32) error {
	query := "chain_id = ? AND apply_height > ?"
	args := []any{chainID, height}
	if part != biz.WholeBlock {
		query = "chain_id = ? AND (apply_height > ? OR (apply_height = ? AND part > ?))"
		args = []any{chainID, height, height, part}
	}
	if err := repo.deleteTrades(ctx, chainID, height, query, args); err != nil {
		return fmt.Errorf("clickhouse store: discard wallet trades: %w", err)
	}
	return nil
}

// deleteTrades 删掉 wallet_trades 里符合 query 的流水，并在 wallet_invalidations 里记下其中
// 最早的成交日，让下次汇总重算那些日子（汇总可能已经读过这些流水）。height 是删除后保留的
// 最高高度。
//
// 失效记录的时间必须晚于删除完成：开始得更早的汇总可能读到了删除前的数据，下次汇总只看
// 它开始之后的记录。所以删除前先把范围存进 PostgreSQL 的 wallet_pending_invalidations，
// 删除后再写进 ClickHouse 并清掉。中途崩溃时行已经删了、重做找不到它们，靠存下的范围补写。
func (repo *walletRepo) deleteTrades(ctx context.Context, chainID uint64, height int64, query string, args []any) error {
	var row struct {
		Earliest time.Time
		Found    uint64
	}
	db := repo.market.WithContext(ctx)
	if err := db.Model(&chdb.WalletTradeRow{}).Select("min(block_time) AS earliest, count() AS found").
		Where(query, args...).Scan(&row).Error; err != nil {
		return err
	}
	if row.Found > 0 {
		if err := repo.state.WithContext(ctx).Exec(`INSERT INTO wallet_pending_invalidations (chain_id, day, height)
			VALUES (?, ?, ?) ON CONFLICT (chain_id) DO UPDATE SET
				day = LEAST(wallet_pending_invalidations.day, EXCLUDED.day),
				height = LEAST(wallet_pending_invalidations.height, EXCLUDED.height)`,
			int64(chainID), row.Earliest.UTC().Truncate(24*time.Hour), height).Error; err != nil {
			return err
		}
		if err := db.Where(query, args...).Delete(&chdb.WalletTradeRow{}).Error; err != nil {
			return err
		}
	}
	return repo.publishInvalidation(ctx, chainID)
}

// publishInvalidation 把存下的失效范围写进 wallet_invalidations 并清掉；没有时什么都不做。
func (repo *walletRepo) publishInvalidation(ctx context.Context, chainID uint64) error {
	var pending []struct {
		Day    time.Time
		Height int64
	}
	state := repo.state.WithContext(ctx)
	if err := state.Raw(`SELECT day, height FROM wallet_pending_invalidations WHERE chain_id = ?`, int64(chainID)).
		Scan(&pending).Error; err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	rows := []chdb.WalletInvalidationRow{{
		ChainID: chainID, At: uint64(time.Now().UnixMilli()), Day: pending[0].Day.UTC(), Height: pending[0].Height,
	}}
	if err := chdb.Insert(ctx, repo.market, &rows); err != nil {
		return err
	}
	return state.Exec(`DELETE FROM wallet_pending_invalidations WHERE chain_id = ?`, int64(chainID)).Error
}

func (repo *walletRepo) Positions(ctx context.Context, chainID uint64, keys []model.WalletKey) ([]model.Position, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	wallets, tokens := make([]string, len(keys)), make([]string, len(keys))
	for index, key := range keys {
		wallets[index], tokens[index] = key.Wallet, key.Token
	}
	var rows []pgdb.PositionRow
	err := repo.state.WithContext(ctx).Raw(`SELECT `+pgdb.PositionColumns+`
		FROM unnest(CAST(@wallets AS text[]), CAST(@tokens AS text[])) AS k(wallet, token)
		JOIN wallet_positions p ON p.chain_id = @chain AND p.wallet = k.wallet AND p.token = k.token`,
		map[string]any{"chain": int64(chainID), "wallets": pgdb.Array(wallets), "tokens": pgdb.Array(tokens)}).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: wallet positions: %w", err)
	}
	return pgdb.Positions(rows)
}

// undoRow 是写入 wallet_undo 的 JSON 形状。
type undoRow struct {
	BlockHeight int64             `json:"block_height"`
	Wallet      string            `json:"wallet"`
	Token       string            `json:"token"`
	Prev        *pgdb.PositionRow `json:"prev"`
}

// upsertPositions 按列名写入持仓。
const upsertPositions = `INSERT INTO wallet_positions
	SELECT * FROM jsonb_populate_recordset(NULL::wallet_positions, CAST(@rows AS jsonb))
	ON CONFLICT (chain_id, wallet, token) DO UPDATE SET
		quantity = EXCLUDED.quantity, cost_usd = EXCLUDED.cost_usd, realized_usd = EXCLUDED.realized_usd,
		bought_qty = EXCLUDED.bought_qty, sold_qty = EXCLUDED.sold_qty, bought_usd = EXCLUDED.bought_usd,
		sold_usd = EXCLUDED.sold_usd, buys = EXCLUDED.buys, sells = EXCLUDED.sells, wins = EXCLUDED.wins,
		losses = EXCLUDED.losses, first_buy_height = EXCLUDED.first_buy_height,
		first_buy_time = EXCLUDED.first_buy_time, opened_at = EXCLUDED.opened_at,
		last_trade_time = EXCLUDED.last_trade_time, updated_height = EXCLUDED.updated_height`

func (repo *walletRepo) CommitWallet(ctx context.Context, commit biz.Commit) error {
	checkpoint := commit.Checkpoint
	if checkpoint.ChainID == 0 {
		return errors.New("wallet store: checkpoint chain id is required")
	}
	if err := chdb.InsertWalletTrades(ctx, repo.market, commit.Trades); err != nil {
		return err
	}
	positions, err := pgdb.MarshalPositions(checkpoint.ChainID, commit.Positions)
	if err != nil {
		return err
	}
	undo := make([]undoRow, len(commit.Undo))
	for index, entry := range commit.Undo {
		undo[index] = undoRow{BlockHeight: entry.Height, Wallet: entry.Key.Wallet, Token: entry.Key.Token}
		if entry.Prev != nil {
			prev := pgdb.NewPositionRow(checkpoint.ChainID, *entry.Prev)
			undo[index].Prev = &prev
		}
	}
	undoJSON, err := json.Marshal(undo)
	if err != nil {
		return fmt.Errorf("wallet store: encode undo: %w", err)
	}
	chainID := int64(checkpoint.ChainID)
	err = repo.state.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := pgdb.Runner{Tx: tx}
		if len(commit.Positions) > 0 {
			run.Exec(upsertPositions, map[string]any{"rows": positions})
		}
		if len(commit.Undo) > 0 {
			// 同一高度跨两次提交改动同一持仓时（区块分片），保留第一次记下的状态。
			run.Exec(`INSERT INTO wallet_undo (chain_id, block_height, wallet, token, prev)
				SELECT @chain, u.block_height, u.wallet, u.token, u.prev
				FROM jsonb_to_recordset(CAST(@undo AS jsonb)) AS u(block_height bigint, wallet text, token text, prev jsonb)
				ON CONFLICT DO NOTHING`, map[string]any{"chain": chainID, "undo": string(undoJSON)})
		}
		run.Exec(`DELETE FROM wallet_undo WHERE chain_id = ? AND block_height <= ?`, chainID, checkpoint.PrunedThrough)
		if run.Err != nil {
			return run.Err
		}
		return saveCheckpoint(tx, checkpoint)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit wallet chain %d height %d: %w", checkpoint.ChainID, checkpoint.Height, err)
	}
	return nil
}

func (repo *walletRepo) RevertWallet(ctx context.Context, to int64, checkpoint biz.Checkpoint) error {
	if err := repo.deleteTrades(ctx, checkpoint.ChainID, to, "chain_id = ? AND apply_height > ?",
		[]any{checkpoint.ChainID, to}); err != nil {
		return fmt.Errorf("clickhouse store: revert wallet trades: %w", err)
	}
	values := map[string]any{"chain": int64(checkpoint.ChainID), "to": to}
	err := repo.state.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := pgdb.Runner{Tx: tx}
		// 先删掉回滚范围内改动过的持仓，再插回每个持仓在范围内第一次改动之前的样子。
		run.Exec(`DELETE FROM wallet_positions p
			USING (SELECT DISTINCT wallet, token FROM wallet_undo WHERE chain_id = @chain AND block_height > @to) u
			WHERE p.chain_id = @chain AND p.wallet = u.wallet AND p.token = u.token`, values)
		run.Exec(`INSERT INTO wallet_positions
			SELECT r.* FROM (
				SELECT DISTINCT ON (wallet, token) prev FROM wallet_undo
				WHERE chain_id = @chain AND block_height > @to ORDER BY wallet, token, block_height
			) u, jsonb_populate_record(NULL::wallet_positions, u.prev) r
			WHERE u.prev IS NOT NULL`, values)
		run.Exec(`DELETE FROM wallet_undo WHERE chain_id = @chain AND block_height > @to`, values)
		if run.Err != nil {
			return run.Err
		}
		return saveCheckpoint(tx, checkpoint)
	})
	if err != nil {
		return fmt.Errorf("postgres store: revert wallet chain %d to %d: %w", checkpoint.ChainID, to, err)
	}
	return nil
}

// tradeSource 读 market-engine 写在 ClickHouse 里的 trades 和 blocks。
type tradeSource struct {
	db *gorm.DB
}

// NewTradeSource 返回补缺用的成交来源。
func NewTradeSource(data *Data) biz.TradeSource { return &tradeSource{db: data.market} }

func (trades *tradeSource) FirstHeightSince(ctx context.Context, chainID uint64, since time.Time) (int64, bool, error) {
	var row struct {
		Height int64
		Found  uint64
	}
	err := trades.db.WithContext(ctx).Raw(`SELECT min(block_height) AS height, count() AS found FROM blocks
		WHERE chain_id = ? AND block_time >= ?`, chainID, since.UTC()).Scan(&row).Error
	if err != nil {
		return 0, false, fmt.Errorf("clickhouse store: first height since: %w", err)
	}
	return row.Height, row.Found > 0, nil
}

func (trades *tradeSource) TradesBetween(ctx context.Context, chainID uint64, from, to int64) ([]biz.Trade, error) {
	var rows []chdb.TradeRow
	err := trades.db.WithContext(ctx).
		Select(`revision_height, block_height, block_time, transaction_hash, log_index, operation_index, pool_id,
			trader, side, base_token, quote_token, base_amount, price_usd, value_usd, outlier`).
		// 按 revision_height 找，不限 block_height 的下界：等待代币精度的成交由之后的区块写出，
		// 延迟多少由 market-engine 的 backfill_blocks 决定。trades 上的 revision_height minmax
		// 索引让它不用扫全表。
		Where(`chain_id = ? AND revision_height BETWEEN ? AND ? AND block_height <= ? AND side IN ('buy', 'sell')`,
			chainID, from, to, to).
		Order("revision_height, block_height, log_index, operation_index").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: trades %d-%d: %w", from, to, err)
	}
	result := make([]biz.Trade, len(rows))
	for index, row := range rows {
		result[index] = biz.Trade{
			RevisionHeight: row.RevisionHeight, BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.UTC(),
			TransactionHash: row.TransactionHash, LogIndex: row.LogIndex, OperationIndex: row.OperationIndex,
			PoolID: row.PoolID, Trader: row.Trader, Side: model.ParseSide(row.Side), BaseToken: row.BaseToken,
			QuoteToken: row.QuoteToken, BaseAmount: row.BaseAmount, PriceUSD: row.PriceUSD, ValueUSD: row.ValueUSD,
			Outlier: row.Outlier,
		}
	}
	return result, nil
}
