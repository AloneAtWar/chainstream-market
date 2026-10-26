package chstore

import (
	"time"

	"github.com/shopspring/decimal"
)

// The models below are the rows market-engine writes, one per table. Their
// fields follow the table columns; enums are written by name.

type blockRow struct {
	ChainID     uint64
	BlockHeight int64
	BlockHash   string
	BlockTime   time.Time
	Finalized   bool
}

func (blockRow) TableName() string { return "blocks" }

type tradeRow struct {
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
	Side             string
	BaseToken        string
	QuoteToken       string
	BaseAmount       decimal.Decimal
	QuoteAmount      decimal.Decimal
	PriceQuote       decimal.Decimal
	PriceUSD         decimal.Decimal `gorm:"column:price_usd"`
	ValueUSD         decimal.Decimal `gorm:"column:value_usd"`
	Finalized        bool
	RevisionHeight   int64
	Outlier          bool
}

func (tradeRow) TableName() string { return "trades" }

type priceRow struct {
	ChainID        uint64
	Token          string
	BlockHeight    int64
	BlockTime      time.Time
	PriceUSD       decimal.Decimal `gorm:"column:price_usd"`
	Source         string
	Hops           uint8
	LiquidityUSD   decimal.Decimal `gorm:"column:liquidity_usd"`
	RevisionHeight int64
}

func (priceRow) TableName() string { return "token_prices" }

type candleRow struct {
	ChainID         uint64
	Subject         string
	Key             string
	IntervalSeconds uint32
	OpenTime        time.Time
	Open            decimal.Decimal
	High            decimal.Decimal
	Low             decimal.Decimal
	Close           decimal.Decimal
	VolumeBase      decimal.Decimal
	VolumeUSD       decimal.Decimal `gorm:"column:volume_usd"`
	Trades          uint32
	UpdateHeight    int64
}

func (candleRow) TableName() string { return "candles" }
