package chraw

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/shopspring/decimal"
)

func TestHistoryReadsTradesOnceTheLoaderCaughtUp(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	history := NewHistory(store, 0)
	if _, ok, err := history.RawTrades(ctx, 1, []string{token}, 100, 101); err != nil || ok {
		t.Fatalf("RawTrades() before any checkpoint = %v, %v; want not ready", ok, err)
	}
	blocks := []model.RawBlock{testBlock(100), testBlock(101), testBlock(102)}
	blocks[1].Trades[0].Amounts[0].Asset = "0xother"
	if err := chdb.WriteRaw(ctx, store.db, blocks); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	if err := chdb.SaveRawCheckpoint(ctx, store.db, chainsync.Checkpoint{ChainID: 1, Offset: 5, Height: 101}); err != nil {
		t.Fatalf("SaveRawCheckpoint() error = %v", err)
	}
	if _, ok, err := history.RawTrades(ctx, 1, []string{token}, 100, 102); err != nil || ok {
		t.Fatalf("RawTrades() past the checkpoint = %v, %v; want not ready", ok, err)
	}
	trades, ok, err := history.RawTrades(ctx, 1, []string{token, "0xother"}, 100, 101)
	if err != nil || !ok || len(trades) != 2 {
		t.Fatalf("RawTrades() = %d trades, %v, %v; want 2", len(trades), ok, err)
	}
	for index, trade := range trades {
		block := blocks[index]
		want := model.RawTradeAt{Height: block.Height, BlockTime: block.BlockTime, RawTrade: block.Trades[0]}
		want.ReservesAfter = []string{}
		if !reflect.DeepEqual(trade, want) {
			t.Fatalf("trade %d = %+v, want %+v", index, trade, want)
		}
	}
	if trades, _, _ := history.RawTrades(ctx, 1, []string{"0xother"}, 100, 101); len(trades) != 1 || trades[0].Height != 101 {
		t.Fatalf("trades of 0xother = %+v, want the one at 101", trades)
	}
}

func TestHistoryWaitsForTheLoader(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if err := chdb.SaveRawCheckpoint(ctx, store.db, chainsync.Checkpoint{ChainID: 1, Offset: 1, Height: 99}); err != nil {
		t.Fatalf("SaveRawCheckpoint() error = %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := chdb.WriteRaw(ctx, store.db, []model.RawBlock{testBlock(100)}); err != nil {
			t.Errorf("WriteRaw() error = %v", err)
		}
		if err := chdb.SaveRawCheckpoint(ctx, store.db, chainsync.Checkpoint{ChainID: 1, Offset: 2, Height: 100}); err != nil {
			t.Errorf("SaveRawCheckpoint() error = %v", err)
		}
	}()
	trades, ok, err := NewHistory(store, 10*time.Second).RawTrades(ctx, 1, []string{token}, 100, 100)
	if err != nil || !ok || len(trades) != 1 {
		t.Fatalf("RawTrades() = %d trades, %v, %v; want 1 after waiting", len(trades), ok, err)
	}
}

func TestHistoryReadsStoredPrices(t *testing.T) {
	ctx := context.Background()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chdb.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("history_test_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	output, err := chstore.Open(ctx, options)
	if err != nil {
		t.Fatalf("chstore.Open() error = %v", err)
	}
	raw, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if dropErr := raw.db.WithContext(context.Background()).Exec("DROP DATABASE IF EXISTS " + options.Database).Error; dropErr != nil {
			t.Errorf("drop database: %v", dropErr)
		}
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
		if closeErr := output.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})
	for _, height := range []int64{90, 95, 100, 103, 110} {
		blockTime := time.Unix(1_700_000_000+height*12, 0).UTC()
		if writeErr := output.WriteBlock(ctx, model.BlockOutput{
			ChainID: 1, Height: height, BlockTime: blockTime,
			Prices: []model.TokenPrice{{
				ChainID: 1, Token: "0xweth", BlockHeight: height, BlockTime: blockTime,
				PriceUSD: decimal.NewFromInt(height), Source: model.PriceSourceGraph, Hops: 1,
			}},
		}); writeErr != nil {
			t.Fatalf("WriteBlock(%d) error = %v", height, writeErr)
		}
	}
	prices, err := NewHistory(raw, 0).PriceHistory(ctx, 1, []string{"0xweth", "0xnone"}, 100, 105)
	if err != nil {
		t.Fatalf("PriceHistory() error = %v", err)
	}
	points := prices["0xweth"]
	if len(prices) != 1 || len(points) != 3 {
		t.Fatalf("prices = %+v, want 95, 100 and 103 for WETH", prices)
	}
	for index, height := range []int64{95, 100, 103} {
		if points[index].Height != height || !points[index].USD.Equal(decimal.NewFromInt(height)) || points[index].Hops != 1 {
			t.Fatalf("point %d = %+v, want height %d", index, points[index], height)
		}
	}
}
