package data

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

var rankingBase = time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)

func rankingBlock(height int64, at time.Duration, prices []model.TokenPrice, candles []model.Candle) model.BlockOutput {
	blockTime := rankingBase.Add(at)
	for index := range prices {
		prices[index].ChainID, prices[index].BlockHeight, prices[index].BlockTime = 1, height, blockTime
	}
	for index := range candles {
		candles[index].ChainID, candles[index].Subject, candles[index].UpdateHeight = 1, model.CandleSubjectToken, height
	}
	return model.BlockOutput{
		ChainID: 1, Height: height, Hash: fmt.Sprintf("0x%d", height), BlockTime: blockTime,
		Prices: prices, Candles: candles,
	}
}

func tokenPrice(token, price, liquidity string) model.TokenPrice {
	return model.TokenPrice{
		Token: token, PriceUSD: decimal.RequireFromString(price), Source: model.PriceSourceGraph,
		LiquidityUSD: decimal.RequireFromString(liquidity),
	}
}

func tokenCandle(token string, interval, openAt time.Duration, open, closePrice, volume string, trades uint32) model.Candle {
	return model.Candle{
		Key: token, Interval: interval, OpenTime: rankingBase.Add(openAt),
		Open: decimal.RequireFromString(open), High: decimal.RequireFromString(closePrice),
		Low: decimal.RequireFromString(open), Close: decimal.RequireFromString(closePrice),
		VolumeUSD: decimal.RequireFromString(volume), Trades: trades,
	}
}

// openSource connects to CLICKHOUSE_ADDR in a fresh database with
// market-engine's tables, or skips when it is unset.
func openSource(t *testing.T) *rankingSource {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chdb.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("rankings_test_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	db, err := chdb.Connect(context.Background(), options)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.WithContext(context.Background()).Exec("DROP DATABASE IF EXISTS " + options.Database).Error; err != nil {
			t.Errorf("drop database: %v", err)
		}
		if err := chdb.Close(db); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if err := chdb.Migrate(context.Background(), db, chdb.MarketSchema); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return &rankingSource{db: db}
}

// rewind deletes what market-engine wrote above rollbackTo, as its reorg
// does.
func rewind(t *testing.T, source *rankingSource, rollbackTo int64) {
	t.Helper()
	for table, column := range map[string]string{
		"blocks": "block_height", "trades": "revision_height", "token_prices": "revision_height", "candles": "update_height",
	} {
		if err := source.db.Exec("DELETE FROM "+table+" WHERE chain_id = 1 AND "+column+" > ?", rollbackTo).Error; err != nil {
			t.Fatalf("rewind %s: %v", table, err)
		}
	}
}

func TestRankingSourceReadsActivityAndQuotes(t *testing.T) {
	ctx := context.Background()
	store := openSource(t)
	if _, ok, err := store.ChainHead(ctx, 1); err != nil || ok {
		t.Fatalf("empty ChainHead() = %v, %v", ok, err)
	}
	five := 5 * time.Minute
	blocks := []model.BlockOutput{
		// An old bucket outside the window, and an hourly candle that a 5m
		// read must ignore.
		rankingBlock(10, time.Hour, []model.TokenPrice{tokenPrice("0xold", "7", "100")}, []model.Candle{
			tokenCandle("0xmeme", five, time.Hour, "1", "1", "500", 5),
			tokenCandle("0xmeme", time.Hour, time.Hour, "1", "1", "500", 5),
		}),
		rankingBlock(20, 30*time.Hour, []model.TokenPrice{tokenPrice("0xmeme", "1.5", "2000")}, []model.Candle{
			tokenCandle("0xmeme", five, 30*time.Hour, "1", "1.5", "10", 1),
		}),
		// A second version of the same bucket: only it counts.
		rankingBlock(21, 30*time.Hour+time.Minute, []model.TokenPrice{tokenPrice("0xmeme", "1.2", "2500")}, []model.Candle{
			tokenCandle("0xmeme", five, 30*time.Hour, "1", "1.2", "25", 2),
			tokenCandle("0xpepe", five, 30*time.Hour, "0.5", "0.4", "3", 1),
		}),
		rankingBlock(22, 31*time.Hour, []model.TokenPrice{tokenPrice("0xmeme", "2", "3000")}, []model.Candle{
			tokenCandle("0xmeme", five, 31*time.Hour, "1.2", "2", "40", 3),
		}),
	}
	for _, block := range blocks {
		if err := chdb.InsertBlock(ctx, store.db, block); err != nil {
			t.Fatalf("InsertBlock(%d) error = %v", block.Height, err)
		}
	}

	head, ok, err := store.ChainHead(ctx, 1)
	if err != nil || !ok || !head.Equal(rankingBase.Add(31*time.Hour)) {
		t.Fatalf("ChainHead() = %s, %v, %v", head, ok, err)
	}
	activity, err := store.TokenActivity(ctx, 1, five, head.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("TokenActivity() error = %v", err)
	}
	byToken := make(map[string]model.TokenActivity)
	for _, row := range activity {
		byToken[row.Token] = row
	}
	if len(byToken) != 2 {
		t.Fatalf("activity = %+v", activity)
	}
	meme := byToken["0xmeme"]
	if meme.Trades != 5 {
		t.Fatalf("meme activity = %+v", meme)
	}
	assertDecimalEqual(t, meme.VolumeUSD, "65")
	assertDecimalEqual(t, meme.Open, "1")
	assertDecimalEqual(t, meme.Close, "2")
	assertDecimalEqual(t, byToken["0xpepe"].Close, "0.4")

	all, err := store.LatestQuotes(ctx, 1, time.Time{}, nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("LatestQuotes(all) = %+v, %v", all, err)
	}
	// Since the chain's second day, only 0xmeme was priced; 0xold is
	// re-read because the caller lists it.
	recent, err := store.LatestQuotes(ctx, 1, rankingBase.Add(24*time.Hour), nil)
	if err != nil || len(recent) != 1 || recent[0].Token != "0xmeme" || recent[0].BlockHeight != 22 {
		t.Fatalf("LatestQuotes(recent) = %+v, %v", recent, err)
	}
	assertDecimalEqual(t, recent[0].PriceUSD, "2")
	assertDecimalEqual(t, recent[0].LiquidityUSD, "3000")
	if !recent[0].BlockTime.Equal(head) {
		t.Fatalf("quote time = %s", recent[0].BlockTime)
	}
	listed, err := store.LatestQuotes(ctx, 1, rankingBase.Add(24*time.Hour), []string{"0xold", "0xgone"})
	if err != nil || len(listed) != 2 {
		t.Fatalf("LatestQuotes(listed) = %+v, %v", listed, err)
	}

	// After a reorg the latest quote and the bucket fall back.
	rewind(t, store, 21)
	recent, err = store.LatestQuotes(ctx, 1, rankingBase.Add(24*time.Hour), nil)
	if err != nil || len(recent) != 1 || recent[0].BlockHeight != 21 {
		t.Fatalf("LatestQuotes(after rewind) = %+v, %v", recent, err)
	}
	activity, err = store.TokenActivity(ctx, 1, five, rankingBase.Add(7*time.Hour))
	if err != nil {
		t.Fatalf("TokenActivity(after rewind) error = %v", err)
	}
	for _, row := range activity {
		if row.Token == "0xmeme" && (row.Trades != 2 || !row.Close.Equal(decimal.RequireFromString("1.2"))) {
			t.Fatalf("meme after rewind = %+v", row)
		}
	}
	if _, err := store.TokenActivity(ctx, 1, 0, head); err == nil {
		t.Fatal("TokenActivity(zero interval) error = nil")
	}
}

func assertDecimalEqual(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("value = %s, want %s", got, want)
	}
}
