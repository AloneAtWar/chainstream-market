// Package storage defines what market-engine persists and the interfaces its
// ClickHouse, Redis and memory backends implement.
//
// Every write is keyed by (chain, block height) so that replaying a block
// after a crash, or rewinding after a reorg, leaves exactly one copy of each
// fact. The engine saves its checkpoint only after Output.WriteBlock returns,
// so a store must tolerate the same block being written twice.
package storage

import (
	"context"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/shopspring/decimal"
)

// CheckpointStore persists one chainsync.Checkpoint per chain.
type CheckpointStore interface {
	Load(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint chainsync.Checkpoint) error
}

// Output stores what the engine derives from each block.
type Output interface {
	// WriteBlock replaces everything previously written for block.Height.
	WriteBlock(ctx context.Context, block BlockOutput) error
	// Rewind removes everything written above rollbackTo and restores
	// running aggregates, such as open candles, to their state at that height.
	Rewind(ctx context.Context, chainID uint64, rollbackTo int64) error
}

// BlockOutput is everything derived from one block.
type BlockOutput struct {
	ChainID   uint64
	Height    int64
	Hash      string
	BlockTime time.Time
	// Finalized reports whether the block was at or below the chain's
	// finalized height when it was published.
	Finalized bool
	// Trades can include trades from earlier blocks that were held until
	// this block brought their token's decimals. They keep their own
	// BlockHeight; Rewind removes them with this block.
	Trades  []Trade
	Prices  []TokenPrice
	Candles []Candle
}

// Side is the trade direction from the base token's point of view.
type Side uint8

const (
	SideUnknown Side = iota
	SideBuy
	SideSell
	SideLiquidityAdded
	SideLiquidityRemoved
)

// Trade is one pool operation oriented to a base/quote pair and valued in USD.
type Trade struct {
	ChainID          uint64
	BlockHeight      int64
	BlockTime        time.Time
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	OperationIndex   uint32
	Protocol         string
	PoolID           string
	Trader           string
	Side             Side
	BaseToken        string
	QuoteToken       string
	BaseAmount       decimal.Decimal
	QuoteAmount      decimal.Decimal
	// PriceQuote is quote per base; PriceUSD and ValueUSD are zero when no
	// USD path exists for the quote token.
	PriceQuote decimal.Decimal
	PriceUSD   decimal.Decimal
	ValueUSD   decimal.Decimal
	// Outlier marks a swap priced too far from its base token's reference
	// price; it is left out of the token's candles.
	Outlier bool
}

// PriceSource says how a token's USD price was obtained.
type PriceSource uint8

const (
	PriceSourceUnknown PriceSource = iota
	// PriceSourceAnchor is a Chainlink-style reference feed.
	PriceSourceAnchor
	// PriceSourceGraph is propagated through liquid pools from an anchor.
	PriceSourceGraph
)

// TokenPrice is a token's USD price as of a block.
type TokenPrice struct {
	ChainID     uint64
	Token       string
	BlockHeight int64
	BlockTime   time.Time
	PriceUSD    decimal.Decimal
	Source      PriceSource
	// Hops is the graph distance from the nearest anchor; zero for anchors.
	Hops uint8
	// LiquidityUSD is the liquidity of the pool the price came through.
	LiquidityUSD decimal.Decimal
}

// CandleSubject says whether a candle aggregates one pool or a whole token.
type CandleSubject uint8

const (
	CandleSubjectUnknown CandleSubject = iota
	CandleSubjectPool
	// CandleSubjectToken aggregates every pool above the liquidity filter.
	CandleSubjectToken
)

// Candle is an OHLCV bucket. A new bucket opens at the previous close.
type Candle struct {
	ChainID      uint64
	Subject      CandleSubject
	Key          string // pool id or token address
	Interval     time.Duration
	OpenTime     time.Time
	Open         decimal.Decimal
	High         decimal.Decimal
	Low          decimal.Decimal
	Close        decimal.Decimal
	VolumeBase   decimal.Decimal
	VolumeUSD    decimal.Decimal
	Trades       uint32
	UpdateHeight int64
}

// Reader serves stored market output to the API.
type Reader interface {
	// LatestPrice returns token's most recent USD price.
	LatestPrice(ctx context.Context, chainID uint64, token string) (TokenPrice, bool, error)
	// Candles returns the current version of each bucket in range, oldest
	// first.
	Candles(ctx context.Context, query CandleQuery) ([]Candle, error)
	// Trades returns the most recent trades, newest first.
	Trades(ctx context.Context, query TradeQuery) ([]Trade, error)
}

// CandleQuery selects buckets with From <= open time < To.
type CandleQuery struct {
	ChainID  uint64
	Subject  CandleSubject
	Key      string
	Interval time.Duration
	From     time.Time
	To       time.Time
	Limit    int
}

// TradeQuery selects trades by base token or pool. BeforeHeight, when
// positive, pages backwards from that height.
type TradeQuery struct {
	ChainID      uint64
	BaseToken    string
	PoolID       string
	BeforeHeight int64
	Limit        int
}

// Revert says what a reorg undid, so live clients can correct what they were
// sent.
type Revert struct {
	ChainID    uint64
	RollbackTo int64
	// Candles lists every candle series a reverted block changed.
	Candles []CandleReset
}

// CandleReset is a candle series as of a revert's rollback height. Latest is
// its newest bucket at that height, or nil when the series had none; any
// newer bucket that the reverted blocks wrote no longer exists.
type CandleReset struct {
	Subject  CandleSubject
	Key      string
	Interval time.Duration
	Latest   *Candle
}
