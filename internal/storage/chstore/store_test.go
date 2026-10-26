package chstore

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

// openTestStore connects to CLICKHOUSE_ADDR (host:port, comma separated) in a
// fresh database, or skips when it is unset.
func openTestStore(t *testing.T) (*Store, chdb.Options) {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chdb.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("market_test_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	store, err := Open(context.Background(), options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.db.WithContext(context.Background()).Exec("DROP DATABASE IF EXISTS " + options.Database).Error; err != nil {
			t.Errorf("drop database: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return store, options
}

func testOutput(height int64, closePrice string) model.BlockOutput {
	blockTime := time.Unix(1_700_000_000+height*12, 0).UTC()
	price := decimal.RequireFromString(closePrice)
	return model.BlockOutput{
		ChainID: 1, Height: height, Hash: fmt.Sprintf("0x%d", height), BlockTime: blockTime,
		Trades: []model.Trade{{
			ChainID: 1, BlockHeight: height, BlockTime: blockTime, TransactionHash: "0xtx",
			LogIndex: 1, Protocol: "uniswap_v2", PoolID: "0xpool", Trader: "0xtrader", Side: model.SideBuy,
			BaseToken: "0xmeme", QuoteToken: "0xweth", BaseAmount: decimal.NewFromInt(100),
			QuoteAmount: decimal.RequireFromString("0.001"), PriceQuote: decimal.RequireFromString("0.00001"),
			PriceUSD: price, ValueUSD: decimal.NewFromInt(3),
		}},
		Prices: []model.TokenPrice{{
			ChainID: 1, Token: "0xmeme", BlockHeight: height, BlockTime: blockTime, PriceUSD: price,
			Source: model.PriceSourceGraph, Hops: 2, LiquidityUSD: decimal.NewFromInt(30000),
		}},
		Candles: []model.Candle{{
			ChainID: 1, Subject: model.CandleSubjectToken, Key: "0xmeme", Interval: time.Hour,
			OpenTime: blockTime.Truncate(time.Hour), Open: decimal.RequireFromString("0.03"),
			High: price, Low: decimal.RequireFromString("0.03"), Close: price,
			VolumeBase: decimal.NewFromInt(100), VolumeUSD: decimal.NewFromInt(3), Trades: uint32(height - 99),
			UpdateHeight: height,
		}},
	}
}

func count(t *testing.T, store *Store, table string) uint64 {
	t.Helper()
	var rows uint64
	if err := store.db.WithContext(context.Background()).Raw("SELECT count() FROM " + table).Row().Scan(&rows); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return rows
}

func assertCounts(t *testing.T, store *Store, want uint64) {
	t.Helper()
	for _, table := range tables {
		if got := count(t, store, table.name); got != want {
			t.Fatalf("%s rows = %d, want %d", table.name, got, want)
		}
	}
}

func TestStoreWritesRewindsAndReplays(t *testing.T) {
	ctx := context.Background()
	store, options := openTestStore(t)
	for _, block := range []model.BlockOutput{testOutput(100, "0.03"), testOutput(101, "0.06")} {
		if err := store.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock(%d) error = %v", block.Height, err)
		}
	}
	assertCounts(t, store, 2)

	// The current candle is the newest version; a rewind restores the older one.
	var closePrice decimal.Decimal
	latest := "SELECT argMax(close, update_height) FROM candles WHERE key = '0xmeme'"
	if err := store.db.WithContext(ctx).Raw(latest).Row().Scan(&closePrice); err != nil {
		t.Fatalf("latest candle: %v", err)
	}
	assertDecimalEqual(t, closePrice, "0.06")
	if err := store.Rewind(ctx, 1, 100); err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	assertCounts(t, store, 1)
	if err := store.db.WithContext(ctx).Raw(latest).Row().Scan(&closePrice); err != nil {
		t.Fatalf("latest candle: %v", err)
	}
	assertDecimalEqual(t, closePrice, "0.03")

	// Replaying a stored height replaces it, also across a reopen.
	if err := store.WriteBlock(ctx, testOutput(100, "0.03")); err != nil {
		t.Fatalf("replay WriteBlock() error = %v", err)
	}
	assertCounts(t, store, 1)
	reopened, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	if reopened.written[1] != 100 {
		t.Fatalf("reopened written height = %d, want 100", reopened.written[1])
	}
	if err := reopened.WriteBlock(ctx, testOutput(100, "0.03")); err != nil {
		t.Fatalf("reopened WriteBlock() error = %v", err)
	}
	assertCounts(t, store, 1)

	var side, source string
	var priceUSD decimal.Decimal
	if err := store.db.WithContext(ctx).Raw("SELECT toString(side), price_usd FROM trades").Row().Scan(&side, &priceUSD); err != nil {
		t.Fatalf("read trade: %v", err)
	}
	if err := store.db.WithContext(ctx).Raw("SELECT toString(source) FROM token_prices").Row().Scan(&source); err != nil {
		t.Fatalf("read price: %v", err)
	}
	if side != "buy" || source != "graph" {
		t.Fatalf("side = %q, source = %q", side, source)
	}
	assertDecimalEqual(t, priceUSD, "0.03")
}

func TestOpenValidatesOptions(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(ctx, chdb.Options{Database: "market"}); err == nil {
		t.Fatal("Open() without address error = nil")
	}
	if _, err := Open(ctx, chdb.Options{Addr: []string{"127.0.0.1:9000"}, Database: "market; DROP"}); err == nil {
		t.Fatal("Open() with invalid database error = nil")
	}
}

func assertDecimalEqual(t *testing.T, got decimal.Decimal, want string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("value = %s, want %s", got, want)
	}
}

// A trade replayed by a later block, and a price that block recomputed,
// keep their own height but belong to the block that wrote them.
func TestReplayedTradesFollowTheirRevision(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	replaying := testOutput(101, "0.06")
	late := testOutput(100, "0.03").Trades[0]
	late.LogIndex = 2
	replaying.Trades = append(replaying.Trades, late)
	recomputed := testOutput(100, "0.03").Prices[0]
	recomputed.Token = "0xlate"
	replaying.Prices = append(replaying.Prices, recomputed)
	for _, block := range []model.BlockOutput{testOutput(100, "0.03"), replaying, replaying} {
		if err := store.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock(%d) error = %v", block.Height, err)
		}
	}
	atHeight := func() uint64 {
		var rows uint64
		if err := store.db.WithContext(ctx).Raw("SELECT count() FROM trades WHERE block_height = 100").Row().Scan(&rows); err != nil {
			t.Fatalf("count trades: %v", err)
		}
		return rows
	}
	pricesAtHeight := func() uint64 {
		var rows uint64
		if err := store.db.WithContext(ctx).Raw("SELECT count() FROM token_prices WHERE block_height = 100").Row().Scan(&rows); err != nil {
			t.Fatalf("count prices: %v", err)
		}
		return rows
	}
	if got, prices := atHeight(), pricesAtHeight(); got != 2 || prices != 2 {
		t.Fatalf("trades and prices at 100 after replay = %d, %d; want 2, 2", got, prices)
	}
	if err := store.Rewind(ctx, 1, 100); err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	if got, prices := atHeight(), pricesAtHeight(); got != 1 || prices != 1 {
		t.Fatalf("trades and prices at 100 after rewind = %d, %d; want 1, 1", got, prices)
	}
}

// Tables created before revision_height gain it, defaulting to block_height.
func TestMigrateAddsRevisionHeight(t *testing.T) {
	ctx := context.Background()
	store, options := openTestStore(t)
	for _, table := range []string{"trades", "token_prices"} {
		if err := store.db.WithContext(ctx).Exec("ALTER TABLE " + table + " DROP COLUMN revision_height").Error; err != nil {
			t.Fatalf("drop %s column: %v", table, err)
		}
	}
	if err := store.db.WithContext(ctx).Exec(`INSERT INTO token_prices (chain_id, token, block_height, block_time)
		VALUES (1, '0xmeme', 100, now()), (1, '0xmeme', 101, now())`).Error; err != nil {
		t.Fatalf("insert old prices: %v", err)
	}
	if err := store.db.WithContext(ctx).Exec(`INSERT INTO trades (chain_id, block_height, block_time, side)
		VALUES (1, 100, now(), 'buy'), (1, 101, now(), 'buy')`).Error; err != nil {
		t.Fatalf("insert old rows: %v", err)
	}
	reopened, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	if err := reopened.Rewind(ctx, 1, 100); err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	var revision int64
	if err := store.db.WithContext(ctx).Raw("SELECT max(revision_height) FROM trades").Row().Scan(&revision); err != nil {
		t.Fatalf("read revision: %v", err)
	}
	if got := count(t, store, "trades"); got != 1 || revision != 100 {
		t.Fatalf("trades = %d, revision = %d; want 1 row at 100", got, revision)
	}
	if got := count(t, store, "token_prices"); got != 1 {
		t.Fatalf("token prices = %d, want 1 row at 100", got)
	}
}
