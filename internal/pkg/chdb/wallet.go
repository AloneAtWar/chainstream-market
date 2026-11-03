package chdb

import (
	"context"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// WalletSchema 建 market-wallet 的 ClickHouse 表，market-api 也读它们。
//
//   - wallet_trades：每笔成交在钱包账本里的结果，按（链, 钱包, 时间）排序，钱包页按它翻页。
//     apply_height 和 part 是带来这笔成交的 live 记录，重组和崩溃恢复按它们删除。
//   - wallet_token_daily：钱包 × 代币 × 天的汇总，market-wallet 每次汇总删掉变化的日子重算。
//   - wallet_stats：钱包在一个窗口（天）内的统计，每次汇总写一个新版本；读的一方只读
//     wallet_refreshes 里最新的版本，旧版本合并掉或按 TTL 过期。
//   - wallet_refreshes：每次汇总一行。
var WalletSchema = []string{
	`CREATE TABLE IF NOT EXISTS wallet_trades (
		chain_id         UInt64,
		apply_height     Int64,
		part             Int32,
		block_height     Int64,
		block_time       DateTime('UTC'),
		transaction_hash String,
		log_index        UInt32,
		operation_index  UInt32,
		wallet           String,
		token            String,
		quote_token      String,
		pool_id          String,
		side             Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2),
		event            Enum8('unknown' = 0, 'open' = 1, 'add' = 2, 'reduce' = 3, 'close' = 4, 'untracked' = 5),
		amount           Decimal256(36),
		value_usd        Decimal256(18),
		price_usd        Decimal256(18),
		realized_usd     Decimal256(18),
		cost_sold_usd    Decimal256(18),
		counted          Bool,
		hold_seconds     Int64,
		quantity_after   Decimal256(36),
		cost_after       Decimal256(18)
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(block_time)
	ORDER BY (chain_id, wallet, block_time, block_height, log_index, operation_index)
	SETTINGS ttl_only_drop_parts = 1`,
	`ALTER TABLE wallet_trades ADD INDEX IF NOT EXISTS wallet_trades_apply apply_height TYPE minmax GRANULARITY 4`,
	`ALTER TABLE wallet_trades ADD INDEX IF NOT EXISTS wallet_trades_token token TYPE bloom_filter GRANULARITY 4`,

	`CREATE TABLE IF NOT EXISTS wallet_token_daily (
		chain_id      UInt64,
		day           Date,
		wallet        String,
		token         String,
		buys          UInt64,
		sells         UInt64,
		bought_usd    Decimal256(18),
		sold_usd      Decimal256(18),
		realized_usd  Decimal256(18),
		cost_sold_usd Decimal256(18),
		wins          UInt64,
		counted       UInt64,
		closes        UInt64,
		hold_seconds  UInt64,
		fast_closes   UInt64
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(day)
	ORDER BY (chain_id, day, wallet, token)`,

	`CREATE TABLE IF NOT EXISTS wallet_stats (
		chain_id         UInt64,
		window_days      UInt16,
		wallet           String,
		realized_usd     Decimal256(18),
		bought_usd       Decimal256(18),
		sold_usd         Decimal256(18),
		buys             UInt64,
		sells            UInt64,
		wins             UInt64,
		counted          UInt64,
		tokens           UInt64,
		tokens_over_5x   UInt64,
		tokens_2x_to_5x  UInt64,
		tokens_0_to_2x   UInt64,
		tokens_loss_50   UInt64,
		tokens_loss_over UInt64,
		closes           UInt64,
		hold_seconds     UInt64,
		fast_closes      UInt64,
		version          UInt64
	) ENGINE = ReplacingMergeTree(version)
	ORDER BY (chain_id, window_days, wallet)
	TTL toDateTime(intDiv(version, 1000)) + INTERVAL 7 DAY`,

	`CREATE TABLE IF NOT EXISTS wallet_refreshes (
		chain_id     UInt64,
		version      UInt64,
		head_time    DateTime('UTC'),
		apply_height Int64
	) ENGINE = ReplacingMergeTree(version)
	ORDER BY chain_id`,
}

// WalletTradeRow 是 wallet_trades 的一行。
type WalletTradeRow struct {
	ChainID         uint64
	ApplyHeight     int64
	Part            int32
	BlockHeight     int64
	BlockTime       time.Time
	TransactionHash string
	LogIndex        uint32
	OperationIndex  uint32
	Wallet          string
	Token           string
	QuoteToken      string
	PoolID          string
	Side            string
	Event           string
	Amount          decimal.Decimal
	ValueUSD        decimal.Decimal `gorm:"column:value_usd"`
	PriceUSD        decimal.Decimal `gorm:"column:price_usd"`
	RealizedUSD     decimal.Decimal `gorm:"column:realized_usd"`
	CostSoldUSD     decimal.Decimal `gorm:"column:cost_sold_usd"`
	Counted         bool
	HoldSeconds     int64
	QuantityAfter   decimal.Decimal
	CostAfter       decimal.Decimal
}

// TableName 返回表名。
func (WalletTradeRow) TableName() string { return "wallet_trades" }

// WalletTrade 把行转回模型。
func (row WalletTradeRow) WalletTrade() model.WalletTrade {
	return model.WalletTrade{
		ChainID: row.ChainID, ApplyHeight: row.ApplyHeight, Part: row.Part, BlockHeight: row.BlockHeight,
		BlockTime: row.BlockTime.UTC(), TransactionHash: row.TransactionHash, LogIndex: row.LogIndex,
		OperationIndex: row.OperationIndex, Wallet: row.Wallet, Token: row.Token, QuoteToken: row.QuoteToken,
		PoolID: row.PoolID, Side: model.ParseSide(row.Side), Event: model.ParseWalletEvent(row.Event),
		Amount: row.Amount, ValueUSD: row.ValueUSD, PriceUSD: row.PriceUSD, RealizedUSD: row.RealizedUSD,
		CostSoldUSD: row.CostSoldUSD, Counted: row.Counted, HoldSeconds: row.HoldSeconds,
		QuantityAfter: row.QuantityAfter, CostAfter: row.CostAfter,
	}
}

// InsertWalletTrades 把账本流水作为一个批次写入 wallet_trades。
func InsertWalletTrades(ctx context.Context, db *gorm.DB, trades []model.WalletTrade) error {
	if len(trades) == 0 {
		return nil
	}
	rows := make([]WalletTradeRow, len(trades))
	for index, trade := range trades {
		rows[index] = WalletTradeRow{
			ChainID: trade.ChainID, ApplyHeight: trade.ApplyHeight, Part: trade.Part, BlockHeight: trade.BlockHeight,
			BlockTime: trade.BlockTime, TransactionHash: trade.TransactionHash, LogIndex: trade.LogIndex,
			OperationIndex: trade.OperationIndex, Wallet: trade.Wallet, Token: trade.Token, QuoteToken: trade.QuoteToken,
			PoolID: trade.PoolID, Side: trade.Side.String(), Event: trade.Event.String(), Amount: trade.Amount,
			ValueUSD: trade.ValueUSD, PriceUSD: trade.PriceUSD, RealizedUSD: trade.RealizedUSD,
			CostSoldUSD: trade.CostSoldUSD, Counted: trade.Counted, HoldSeconds: trade.HoldSeconds,
			QuantityAfter: trade.QuantityAfter, CostAfter: trade.CostAfter,
		}
	}
	if err := Insert(ctx, db, &rows); err != nil {
		return fmt.Errorf("clickhouse store: insert wallet_trades: %w", err)
	}
	return nil
}

// WalletStatsRow 是 wallet_stats 的一行。
type WalletStatsRow struct {
	ChainID        uint64
	WindowDays     uint16
	Wallet         string
	RealizedUSD    decimal.Decimal `gorm:"column:realized_usd"`
	BoughtUSD      decimal.Decimal `gorm:"column:bought_usd"`
	SoldUSD        decimal.Decimal `gorm:"column:sold_usd"`
	Buys           uint64
	Sells          uint64
	Wins           uint64
	Counted        uint64
	Tokens         uint64
	TokensOver5x   uint64 `gorm:"column:tokens_over_5x"`
	Tokens2xTo5x   uint64 `gorm:"column:tokens_2x_to_5x"`
	Tokens0To2x    uint64 `gorm:"column:tokens_0_to_2x"`
	TokensLoss50   uint64 `gorm:"column:tokens_loss_50"`
	TokensLossOver uint64 `gorm:"column:tokens_loss_over"`
	Closes         uint64
	HoldSeconds    uint64
	FastCloses     uint64
	Version        uint64
}

// TableName 返回表名。
func (WalletStatsRow) TableName() string { return "wallet_stats" }

// WalletStats 把行转回模型。
func (row WalletStatsRow) WalletStats() model.WalletStats {
	return model.WalletStats{
		ChainID: row.ChainID, WindowDays: row.WindowDays, Wallet: row.Wallet, RealizedUSD: row.RealizedUSD,
		BoughtUSD: row.BoughtUSD, SoldUSD: row.SoldUSD, Buys: row.Buys, Sells: row.Sells, Wins: row.Wins,
		Counted: row.Counted, Tokens: row.Tokens, TokensOver5x: row.TokensOver5x, Tokens2xTo5x: row.Tokens2xTo5x,
		Tokens0To2x: row.Tokens0To2x, TokensLoss50: row.TokensLoss50, TokensLossOver: row.TokensLossOver,
		Closes: row.Closes, HoldSeconds: row.HoldSeconds, FastCloses: row.FastCloses,
		RefreshedAt: time.UnixMilli(int64(row.Version)).UTC(),
	}
}

// WalletRefreshRow 是 wallet_refreshes 的一行。
type WalletRefreshRow struct {
	ChainID     uint64
	Version     uint64
	HeadTime    time.Time
	ApplyHeight int64
}

// TableName 返回表名。
func (WalletRefreshRow) TableName() string { return "wallet_refreshes" }

// LatestWalletRefresh 返回链最近一次汇总，没有时 ok 为 false。
func LatestWalletRefresh(ctx context.Context, db *gorm.DB, chainID uint64) (WalletRefreshRow, bool, error) {
	var rows []WalletRefreshRow
	err := db.WithContext(ctx).Where("chain_id = ?", chainID).Order("version DESC").Limit(1).Find(&rows).Error
	if err != nil {
		return WalletRefreshRow{}, false, fmt.Errorf("clickhouse store: latest wallet refresh: %w", err)
	}
	if len(rows) == 0 {
		return WalletRefreshRow{}, false, nil
	}
	return rows[0], true, nil
}

// SetWalletTradesTTL 让 wallet_trades 的行在成交 days 天后过期；days 为零时去掉 TTL。
func SetWalletTradesTTL(ctx context.Context, db *gorm.DB, days int) error {
	statement := "ALTER TABLE wallet_trades REMOVE TTL"
	if days > 0 {
		statement = fmt.Sprintf("ALTER TABLE wallet_trades MODIFY TTL block_time + INTERVAL %d DAY", days)
	}
	err := db.WithContext(ctx).Exec(statement).Error
	if err != nil && days == 0 {
		// 表上本来就没有 TTL 时 REMOVE TTL 会报错，可以忽略。
		return nil
	}
	if err != nil {
		return fmt.Errorf("clickhouse store: wallet_trades ttl: %w", err)
	}
	return nil
}
