// Package live pushes trades and candles to clients as blocks arrive.
//
// market-engine publishes what each block derived, and every reorg
// correction, to one live topic per chain (Publisher). market-api consumes
// those topics and fans them out to WebSocket clients by channel (Hub).
package live

import (
	"strconv"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// Update types on a live topic.
const (
	UpdateBlock  = "block"
	UpdateRevert = "revert"
)

// Update is one record on a chain's live topic. A block's output, or a
// revert's resets, can span several records; Part numbers them from zero.
type Update struct {
	Type    string `json:"type"`
	ChainID uint64 `json:"chain_id"`
	// Height is the block for UpdateBlock, and the height rolled back to for
	// UpdateRevert.
	Height          int64    `json:"height"`
	Part            int      `json:"part,omitempty"`
	BlockTime       int64    `json:"block_time,omitempty"`
	FinalizedHeight int64    `json:"finalized_height,omitempty"`
	Trades          []Trade  `json:"trades,omitempty"`
	Candles         []Candle `json:"candles,omitempty"`
	Resets          []Reset  `json:"resets,omitempty"`
}

// Trade is a trade as clients receive it. It has the REST trade fields plus
// RevisionHeight, the block that emitted it: a trade held for late token
// decimals keeps its own BlockHeight but is emitted by a later block, and a
// revert to below RevisionHeight retracts it.
type Trade struct {
	BlockHeight     int64  `json:"block_height"`
	BlockTime       int64  `json:"block_time"`
	RevisionHeight  int64  `json:"revision_height"`
	TransactionHash string `json:"transaction_hash"`
	LogIndex        uint32 `json:"log_index"`
	OperationIndex  uint32 `json:"operation_index"`
	Protocol        string `json:"protocol"`
	PoolID          string `json:"pool_id"`
	Trader          string `json:"trader"`
	Side            string `json:"side"`
	BaseToken       string `json:"base_token"`
	QuoteToken      string `json:"quote_token"`
	BaseAmount      string `json:"base_amount"`
	QuoteAmount     string `json:"quote_amount"`
	PriceQuote      string `json:"price_quote"`
	PriceUSD        string `json:"price_usd"`
	ValueUSD        string `json:"value_usd"`
	// Outlier marks a swap left out of the token's candles.
	Outlier bool `json:"outlier"`
}

// Candle is the whole current value of one bucket. UpdateHeight is the block
// that last changed it.
type Candle struct {
	Subject      string `json:"subject"`
	Key          string `json:"key"`
	Interval     string `json:"interval"`
	OpenTime     int64  `json:"open_time"`
	Open         string `json:"open"`
	High         string `json:"high"`
	Low          string `json:"low"`
	Close        string `json:"close"`
	VolumeBase   string `json:"volume_base"`
	VolumeUSD    string `json:"volume_usd"`
	Trades       uint32 `json:"trades"`
	UpdateHeight int64  `json:"update_height"`
}

// Reset is a candle series after a revert: Latest is its newest bucket, or
// nil when it has none, and every newer bucket is gone.
type Reset struct {
	Subject  string  `json:"subject"`
	Key      string  `json:"key"`
	Interval string  `json:"interval"`
	Latest   *Candle `json:"latest"`
}

// IntervalName formats an interval the way the API accepts it, such as
// "1m", "4h" or "1d".
func IntervalName(interval time.Duration) string {
	switch {
	case interval%(24*time.Hour) == 0:
		return strconv.FormatInt(int64(interval/(24*time.Hour)), 10) + "d"
	case interval%time.Hour == 0:
		return strconv.FormatInt(int64(interval/time.Hour), 10) + "h"
	case interval%time.Minute == 0:
		return strconv.FormatInt(int64(interval/time.Minute), 10) + "m"
	default:
		return strconv.FormatInt(int64(interval/time.Second), 10) + "s"
	}
}

func tradeOf(trade model.Trade, revision int64) Trade {
	return Trade{
		BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime.Unix(), RevisionHeight: revision,
		TransactionHash: trade.TransactionHash, LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex,
		Protocol: trade.Protocol, PoolID: trade.PoolID, Trader: trade.Trader, Side: trade.Side.String(),
		BaseToken: trade.BaseToken, QuoteToken: trade.QuoteToken,
		BaseAmount: trade.BaseAmount.String(), QuoteAmount: trade.QuoteAmount.String(),
		PriceQuote: trade.PriceQuote.String(), PriceUSD: trade.PriceUSD.String(), ValueUSD: trade.ValueUSD.String(),
		Outlier: trade.Outlier,
	}
}

func candleOf(candle model.Candle) Candle {
	return Candle{
		Subject: candle.Subject.String(), Key: candle.Key, Interval: IntervalName(candle.Interval),
		OpenTime: candle.OpenTime.Unix(), Open: candle.Open.String(), High: candle.High.String(),
		Low: candle.Low.String(), Close: candle.Close.String(), VolumeBase: candle.VolumeBase.String(),
		VolumeUSD: candle.VolumeUSD.String(), Trades: candle.Trades, UpdateHeight: candle.UpdateHeight,
	}
}

func resetOf(reset model.CandleReset) Reset {
	result := Reset{Subject: reset.Subject.String(), Key: reset.Key, Interval: IntervalName(reset.Interval)}
	if reset.Latest != nil {
		latest := candleOf(*reset.Latest)
		result.Latest = &latest
	}
	return result
}

// TradeChannel names the trades of a pool, or of a token as base, such as
// "trades:1:pool:0xabc".
func TradeChannel(chainID uint64, subject, key string) string {
	return strings.Join([]string{"trades", strconv.FormatUint(chainID, 10), subject, key}, ":")
}

// CandleChannel names one candle series, such as "candles:1:token:0xabc:1m".
func CandleChannel(chainID uint64, subject, key, interval string) string {
	return strings.Join([]string{"candles", strconv.FormatUint(chainID, 10), subject, key, interval}, ":")
}
