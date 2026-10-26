package model

import (
	"time"

	"github.com/shopspring/decimal"
)

// TokenActivity is a token's aggregated candles over the ranking window.
type TokenActivity struct {
	Token     string
	VolumeUSD decimal.Decimal
	Trades    int64
	// Open is the first bucket's open, which is the last price before the
	// window when the token traded earlier; Close is the last bucket's close.
	Open  decimal.Decimal
	Close decimal.Decimal
}

// TokenQuote is a token's latest USD price.
type TokenQuote struct {
	Token        string
	PriceUSD     decimal.Decimal
	LiquidityUSD decimal.Decimal
	BlockHeight  int64
	BlockTime    time.Time
}

// RankingRefresh replaces one chain's market columns in the rankings.
type RankingRefresh struct {
	ChainID uint64
	// Since is where the quote scan started: a ranked token priced at or
	// after it that is missing from Quotes lost its prices to a reorg. A
	// zero Since means Quotes holds every priced token.
	Since    time.Time
	Quotes   []TokenQuote
	Activity []TokenActivity
	// Head is the chain head time the refresh read, kept as the next scan's
	// cursor.
	Head        time.Time
	RefreshedAt time.Time
}

// RankingSort is the column a token list is ordered by.
type RankingSort uint8

const (
	SortVolume24h RankingSort = iota
	SortPriceChange24h
	SortLiquidity
	SortHolders
	SortCreated
)

var rankingSortNames = []string{"volume_24h", "price_change_24h", "liquidity", "holders", "created"}

func (sort RankingSort) String() string {
	if int(sort) < len(rankingSortNames) {
		return rankingSortNames[sort]
	}
	return "unknown"
}

// RankingSorts lists the sorts by their API names.
func RankingSorts() []string { return append([]string{}, rankingSortNames...) }

// ParseRankingSort maps an API name to its sort.
func ParseRankingSort(name string) (RankingSort, bool) {
	for index, candidate := range rankingSortNames {
		if candidate == name {
			return RankingSort(index), true
		}
	}
	return 0, false
}

// TokenListQuery selects a page of the rankings. With Search set, it
// matches symbols by prefix, or one address exactly, among every registry
// token, and exact symbol matches come first.
type TokenListQuery struct {
	ChainID         uint64
	Sort            RankingSort
	Ascending       bool
	Search          string
	MinLiquidityUSD decimal.Decimal
	Limit           int
	Offset          int
}

// RankedToken is one row of a token list. Market fields are zero and Ranked
// is false for a searched token that has no market data.
type RankedToken struct {
	Token    string
	Name     *string
	Symbol   *string
	Decimals *uint8
	Ranked   bool
	// PriceUSD and PriceHeight are nil when the token traded in the window
	// but has no price of its own.
	PriceUSD       *decimal.Decimal
	PriceHeight    *int64
	PriceChange24h decimal.Decimal
	Volume24hUSD   decimal.Decimal
	Trades24h      int64
	LiquidityUSD   decimal.Decimal
	Holders        int64
	// CreatedHeight and CreatedTime come from the token's deployment, when
	// market-registry knows it.
	CreatedHeight *int64
	CreatedTime   *int64
	RefreshedAt   time.Time
}
