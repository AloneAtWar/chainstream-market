package service

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// The readers market-api serves, which the data layer implements over the
// tables the other components write.

// Reader reads market-engine's output.
type Reader interface {
	// LatestPrice returns token's most recent USD price.
	LatestPrice(ctx context.Context, chainID uint64, token string) (model.TokenPrice, bool, error)
	// Candles returns the current version of each bucket in range, oldest
	// first.
	Candles(ctx context.Context, query model.CandleQuery) ([]model.Candle, error)
	// Trades returns the most recent trades, newest first.
	Trades(ctx context.Context, query model.TradeQuery) ([]model.Trade, error)
}

// TransferReader reads ch-loader's raw transfers.
type TransferReader interface {
	Transfers(ctx context.Context, query model.TransferQuery) ([]model.Transfer, error)
}

// HolderReader reads holder-indexer's balances.
type HolderReader interface {
	// TopHolders returns the largest positive holdings of token.
	TopHolders(ctx context.Context, chainID uint64, token string, limit int) ([]model.Holding, error)
	// HolderCount returns the number of holders of token with a positive
	// balance, which holder-indexer keeps rather than counted on each call.
	HolderCount(ctx context.Context, chainID uint64, token string) (int64, error)
}

// RegistryReader reads market-registry's token and pool metadata.
type RegistryReader interface {
	Token(ctx context.Context, chainID uint64, address string) (model.Token, bool, error)
	Pool(ctx context.Context, chainID uint64, poolID string) (model.Pool, bool, error)
	// TokenPools returns pools that trade token, most recently observed
	// first.
	TokenPools(ctx context.Context, chainID uint64, token string, limit int) ([]model.Pool, error)
}

// RankingReader reads market-rankings' token lists.
type RankingReader interface {
	TokenList(ctx context.Context, query model.TokenListQuery) ([]model.RankedToken, error)
}

// LiquidityReader reads the tick tables market-engine keeps for
// concentrated-liquidity pools.
type LiquidityReader interface {
	// PoolTickTable returns a pool's latest committed table; ok is false
	// when market-engine keeps none for it.
	PoolTickTable(ctx context.Context, chainID uint64, poolID string) (model.PoolTickTable, bool, error)
}

// WalletReader 读 market-wallet 的持仓、流水、统计和标签。
type WalletReader interface {
	// WalletStats 返回最近一次汇总的钱包统计，按 query.Sort 降序；还没有汇总时为空。
	WalletStats(ctx context.Context, query model.WalletStatsQuery) ([]model.WalletStats, error)
	// Positions 返回一个钱包（按最近交易时间倒序）或一个代币（按已实现盈亏降序）的持仓。
	Positions(ctx context.Context, query model.PositionQuery) ([]model.PricedPosition, error)
	// WalletTrades 返回钱包的账本流水，最新的在前。
	WalletTrades(ctx context.Context, query model.WalletTradeQuery) ([]model.WalletTrade, error)
	// LabeledWallets 返回带 label 标签的全部钱包。
	LabeledWallets(ctx context.Context, chainID uint64, label string) ([]string, error)
	// WalletLabels 返回 wallets 中各钱包的标签。
	WalletLabels(ctx context.Context, chainID uint64, wallets []string) (map[string][]string, error)
}
