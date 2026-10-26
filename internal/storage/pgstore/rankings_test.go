package pgstore

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

func testAddress(digit string) string { return "0x" + strings.Repeat(digit, 40) }

var (
	tokenAA = testAddress("a")
	tokenBB = testAddress("b")
	tokenCC = testAddress("c")
	tokenDD = testAddress("d")
	tokenEE = testAddress("e")
)

func registryToken(address, symbol string, deployment *model.Deployment) model.Token {
	name := symbol + " token"
	return model.Token{
		Address: address, Standard: "erc20", Recognition: "recognized", MetadataComplete: true,
		Name: &name, Symbol: &symbol, Created: deployment != nil, Deployment: deployment, SourceHeight: 1,
	}
}

func seedHolders(t *testing.T, store *Store, balances map[model.HolderKey]int64) {
	t.Helper()
	change := model.HolderChange{Height: 1}
	for key, balance := range balances {
		change.Created = append(change.Created, model.HolderSeed{
			Holding: model.Holding{HolderKey: key, Balance: big.NewInt(balance)}, Verified: true,
		})
	}
	if err := store.CommitHolders(context.Background(), chainsync.Checkpoint{ChainID: 1, Height: 1}, change); err != nil {
		t.Fatalf("CommitHolders() error = %v", err)
	}
}

func quote(token, price, liquidity string, height int64, at time.Time) model.TokenQuote {
	return model.TokenQuote{
		Token: token, PriceUSD: decimal.RequireFromString(price), LiquidityUSD: decimal.RequireFromString(liquidity),
		BlockHeight: height, BlockTime: at,
	}
}

func activity(token, volume string, trades int64, open, closePrice string) model.TokenActivity {
	return model.TokenActivity{
		Token: token, VolumeUSD: decimal.RequireFromString(volume), Trades: trades,
		Open: decimal.RequireFromString(open), Close: decimal.RequireFromString(closePrice),
	}
}

func listTokens(t *testing.T, store *Store, query model.TokenListQuery) []model.RankedToken {
	t.Helper()
	query.ChainID = 1
	if query.Limit == 0 {
		query.Limit = 10
	}
	tokens, err := store.TokenList(context.Background(), query)
	if err != nil {
		t.Fatalf("TokenList(%+v) error = %v", query, err)
	}
	return tokens
}

func assertOrder(t *testing.T, tokens []model.RankedToken, want ...string) {
	t.Helper()
	got := make([]string, len(tokens))
	for index, token := range tokens {
		got[index] = token.Token
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("tokens = %v, want %v", got, want)
	}
}

func TestRankingsRefreshAndList(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if _, ok, err := store.LoadRankingCursor(ctx, 1); err != nil || ok {
		t.Fatalf("empty LoadRankingCursor() = %v, %v", ok, err)
	}
	if err := store.CommitRegistry(ctx, chainsync.Checkpoint{ChainID: 1, Height: 1}, model.RegistryChange{
		Height: 1,
		Tokens: []model.Token{
			registryToken(tokenAA, "USDC", &model.Deployment{BlockHeight: 5, BlockTime: 50}),
			registryToken(tokenBB, "PEPE", nil),
			registryToken(tokenCC, "pepe2", &model.Deployment{BlockHeight: 9, BlockTime: 90}),
			registryToken(tokenDD, "USDT", nil),
		},
	}); err != nil {
		t.Fatalf("CommitRegistry() error = %v", err)
	}
	seedHolders(t, store, map[model.HolderKey]int64{
		{Token: tokenAA, Holder: "0x1"}: 10, {Token: tokenAA, Holder: "0x2"}: 20,
		{Token: tokenAA, Holder: "0x3"}: 30, {Token: tokenAA, Holder: "0x4"}: 0,
		{Token: tokenBB, Holder: "0x1"}: 5,
	})

	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	refreshedAt := day.Add(time.Minute)
	if err := store.CommitRankings(ctx, model.RankingRefresh{
		ChainID: 1, Head: day, RefreshedAt: refreshedAt,
		Quotes: []model.TokenQuote{
			quote(tokenAA, "1", "5000", 100, day),
			quote(tokenBB, "0.001", "200", 101, day),
			quote(tokenCC, "2", "50", 99, day.Add(-2*time.Hour)),
		},
		Activity: []model.TokenActivity{
			activity(tokenAA, "1000", 10, "1", "1.1"),
			activity(tokenBB, "50", 4, "0.002", "0.001"),
			activity(tokenEE, "5", 1, "3", "3"),
		},
	}); err != nil {
		t.Fatalf("CommitRankings() error = %v", err)
	}

	byVolume := listTokens(t, store, model.TokenListQuery{Sort: model.SortVolume24h})
	assertOrder(t, byVolume, tokenAA, tokenBB, tokenEE, tokenCC)
	top := byVolume[0]
	if !top.Ranked || top.Symbol == nil || *top.Symbol != "USDC" || top.Holders != 3 || top.Trades24h != 10 ||
		top.CreatedHeight == nil || *top.CreatedHeight != 5 || *top.CreatedTime != 50 ||
		top.PriceHeight == nil || *top.PriceHeight != 100 || !top.RefreshedAt.Equal(refreshedAt) {
		t.Fatalf("top token = %+v", top)
	}
	assertDecimalString(t, top.PriceChange24h, "0.1")
	assertDecimalString(t, top.Volume24hUSD, "1000")
	assertDecimalString(t, *top.PriceUSD, "1")
	if traded := byVolume[2]; traded.PriceUSD != nil || traded.Symbol != nil || !traded.LiquidityUSD.IsZero() {
		t.Fatalf("unpriced token = %+v", traded)
	}
	assertDecimalString(t, byVolume[1].PriceChange24h, "-0.5")

	assertOrder(t, listTokens(t, store, model.TokenListQuery{Sort: model.SortPriceChange24h, Ascending: true}),
		tokenBB, tokenCC, tokenEE, tokenAA)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Sort: model.SortLiquidity, Limit: 2, Offset: 1}),
		tokenBB, tokenCC)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Sort: model.SortHolders, Limit: 2}), tokenAA, tokenBB)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Sort: model.SortCreated}), tokenCC, tokenAA)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{MinLiquidityUSD: decimal.NewFromInt(100)}), tokenAA, tokenBB)

	// Symbol search is a case-insensitive prefix match with exact matches
	// first, then ranked tokens, then the sort.
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: "Pepe"}), tokenBB, tokenCC)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: "pepe2"}), tokenCC)
	usd := listTokens(t, store, model.TokenListQuery{Search: "usd"})
	assertOrder(t, usd, tokenAA, tokenDD)
	if usd[1].Ranked || usd[1].Name == nil || usd[1].Holders != 0 {
		t.Fatalf("unranked search result = %+v", usd[1])
	}
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: "%"}))
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: strings.ToUpper(tokenDD)}), tokenDD)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: tokenEE}), tokenEE)
	assertOrder(t, listTokens(t, store, model.TokenListQuery{Search: testAddress("f")}))

	head, ok, err := store.LoadRankingCursor(ctx, 1)
	if err != nil || !ok || !head.Equal(day) {
		t.Fatalf("LoadRankingCursor() = %s, %v, %v", head, ok, err)
	}
	recent, err := store.RankedSince(ctx, 1, day.Add(-time.Hour))
	if err != nil || len(recent) != 2 {
		t.Fatalf("RankedSince() = %v, %v", recent, err)
	}

	// The next refresh rescans from an hour back: 0xbb's price was reorged
	// away and it no longer trades, so it leaves; 0xcc's older price stays;
	// 0xee no longer trades and has no price.
	if err := store.CommitRankings(ctx, model.RankingRefresh{
		ChainID: 1, Since: day.Add(-time.Hour), Head: day.Add(time.Minute), RefreshedAt: refreshedAt.Add(time.Minute),
		Quotes:   []model.TokenQuote{quote(tokenAA, "1.2", "6000", 105, day.Add(time.Minute))},
		Activity: []model.TokenActivity{activity(tokenAA, "1200", 12, "1", "1.2")},
	}); err != nil {
		t.Fatalf("second CommitRankings() error = %v", err)
	}
	after := listTokens(t, store, model.TokenListQuery{Sort: model.SortLiquidity})
	assertOrder(t, after, tokenAA, tokenCC)
	assertDecimalString(t, after[0].LiquidityUSD, "6000")
	assertDecimalString(t, after[0].PriceChange24h, "0.2")
	if after[1].Trades24h != 0 || !after[1].Volume24hUSD.IsZero() || after[1].PriceUSD == nil {
		t.Fatalf("idle token = %+v", after[1])
	}

	// A full scan (zero Since) drops every price it did not return.
	if err := store.CommitRankings(ctx, model.RankingRefresh{ChainID: 1, Head: day.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("empty CommitRankings() error = %v", err)
	}
	assertOrder(t, listTokens(t, store, model.TokenListQuery{}))
	if _, err := store.TokenList(ctx, model.TokenListQuery{ChainID: 1}); err == nil {
		t.Fatal("TokenList() without a limit error = nil")
	}
}

func assertDecimalString(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("decimal = %s, want %s", got, want)
	}
}
