package chdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// InsertBlock inserts block's trades, prices and candles, then its block
// row. It does not delete what an earlier write left at the height.
func InsertBlock(ctx context.Context, db *gorm.DB, block model.BlockOutput) error {
	if block.ChainID == 0 {
		return errors.New("clickhouse store: block chain id is required")
	}
	if err := insertTrades(ctx, db, block); err != nil {
		return err
	}
	if err := insertPrices(ctx, db, block); err != nil {
		return err
	}
	if err := insertCandles(ctx, db, block); err != nil {
		return err
	}
	// The block row goes last: its presence means the height is complete.
	return insertMarket(ctx, db, "blocks", &[]BlockRow{{
		ChainID: block.ChainID, BlockHeight: block.Height, BlockHash: block.Hash,
		BlockTime: block.BlockTime, Finalized: block.Finalized,
	}})
}

func insertTrades(ctx context.Context, db *gorm.DB, block model.BlockOutput) error {
	if len(block.Trades) == 0 {
		return nil
	}
	rows := make([]TradeRow, len(block.Trades))
	for index, trade := range block.Trades {
		rows[index] = TradeRow{
			ChainID: trade.ChainID, BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime,
			TransactionHash: trade.TransactionHash, TransactionIndex: trade.TransactionIndex,
			LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, Protocol: trade.Protocol,
			PoolID: trade.PoolID, Trader: trade.Trader, Side: trade.Side.String(),
			BaseToken: trade.BaseToken, QuoteToken: trade.QuoteToken,
			BaseAmount: trade.BaseAmount, QuoteAmount: trade.QuoteAmount, PriceQuote: trade.PriceQuote,
			PriceUSD: trade.PriceUSD, ValueUSD: trade.ValueUSD, Finalized: block.Finalized,
			RevisionHeight: block.Height, Outlier: trade.Outlier,
		}
	}
	return insertMarket(ctx, db, "trades", &rows)
}

func insertPrices(ctx context.Context, db *gorm.DB, block model.BlockOutput) error {
	if len(block.Prices) == 0 {
		return nil
	}
	rows := make([]TokenPriceRow, len(block.Prices))
	for index, price := range block.Prices {
		rows[index] = TokenPriceRow{
			ChainID: price.ChainID, Token: price.Token, BlockHeight: price.BlockHeight, BlockTime: price.BlockTime,
			PriceUSD: price.PriceUSD, Source: price.Source.String(), Hops: price.Hops,
			LiquidityUSD: price.LiquidityUSD, RevisionHeight: block.Height,
		}
	}
	return insertMarket(ctx, db, "token_prices", &rows)
}

func insertCandles(ctx context.Context, db *gorm.DB, block model.BlockOutput) error {
	if len(block.Candles) == 0 {
		return nil
	}
	rows := make([]CandleRow, len(block.Candles))
	for index, candle := range block.Candles {
		rows[index] = CandleRow{
			ChainID: candle.ChainID, Subject: candle.Subject.String(), Key: candle.Key,
			IntervalSeconds: uint32(candle.Interval / time.Second), OpenTime: candle.OpenTime,
			Open: candle.Open, High: candle.High, Low: candle.Low, Close: candle.Close,
			VolumeBase: candle.VolumeBase, VolumeUSD: candle.VolumeUSD, Trades: candle.Trades,
			UpdateHeight: candle.UpdateHeight,
		}
	}
	return insertMarket(ctx, db, "candles", &rows)
}

func insertMarket(ctx context.Context, db *gorm.DB, table string, rows any) error {
	if err := Insert(ctx, db, rows); err != nil {
		return fmt.Errorf("clickhouse store: insert %s: %w", table, err)
	}
	return nil
}

// The models below are the rows market-engine writes, one per table of
// MarketSchema. Their fields follow the table columns; enums are written by
// name. market-api and market-rankings read them.

// BlockRow is a blocks row. A block's row is written last, so its presence means
// the height is complete.
type BlockRow struct {
	ChainID     uint64
	BlockHeight int64
	BlockHash   string
	BlockTime   time.Time
	Finalized   bool
}

func (BlockRow) TableName() string { return "blocks" }

// TradeRow is a trades row: one oriented trade.
type TradeRow struct {
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

func (TradeRow) TableName() string { return "trades" }

// TokenPriceRow is a token_prices row.
type TokenPriceRow struct {
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

func (TokenPriceRow) TableName() string { return "token_prices" }

// CandleRow is a candles row: one version of a bucket.
type CandleRow struct {
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

func (CandleRow) TableName() string { return "candles" }
