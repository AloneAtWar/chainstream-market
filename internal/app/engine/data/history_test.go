package data

import (
	"context"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

const (
	token = "0x1111111111111111111111111111111111111111"
	alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// testBlock is a block of every raw fact, as ch-loader stores it.
func testBlock(height int64) model.RawBlock {
	bin := uint32(8388608)
	return model.RawBlock{
		ChainID: 1, Height: height, BlockTime: time.Unix(1_700_000_000+height*12, 0).UTC(),
		Transfers: []model.RawTransfer{
			{TransactionHash: "0xt1", LogIndex: 1, Token: token, From: alice, To: bob, Amount: big.NewInt(height)},
		},
		Trades: []model.RawTrade{{
			TransactionHash: "0xt3", LogIndex: 3, Protocol: "uniswap_v3", PoolID: "0xpool", Action: "swap",
			Amounts: []model.RawAmount{
				{Asset: token, Raw: "-100", Value: "-0.0001", Decimals: 6, DecimalsKnown: true},
				{Asset: "0xweth", Raw: "5"},
			},
			SqrtPriceX96: "79228162514264337593543950336", Liquidity: "1", Tick: "0", ActiveBinID: &bin,
		}},
	}
}

func TestHistoryReadsTradesOnceTheLoaderCaughtUp(t *testing.T) {
	ctx := context.Background()
	db, _ := openClickHouse(t, chdb.RawSchema)
	history := NewHistoryRepo(db, 0)
	if _, ok, err := history.RawTrades(ctx, 1, []string{token}, 100, 101); err != nil || ok {
		t.Fatalf("RawTrades() before any checkpoint = %v, %v; want not ready", ok, err)
	}
	blocks := []model.RawBlock{testBlock(100), testBlock(101), testBlock(102)}
	blocks[1].Trades[0].Amounts[0].Asset = "0xother"
	if err := chdb.WriteRaw(ctx, db, blocks); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	if err := chdb.SaveRawCheckpoint(ctx, db, chainsync.Checkpoint{ChainID: 1, Offset: 5, Height: 101}); err != nil {
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
	db, _ := openClickHouse(t, chdb.RawSchema)
	if err := chdb.SaveRawCheckpoint(ctx, db, chainsync.Checkpoint{ChainID: 1, Offset: 1, Height: 99}); err != nil {
		t.Fatalf("SaveRawCheckpoint() error = %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := chdb.WriteRaw(ctx, db, []model.RawBlock{testBlock(100)}); err != nil {
			t.Errorf("WriteRaw() error = %v", err)
		}
		if err := chdb.SaveRawCheckpoint(ctx, db, chainsync.Checkpoint{ChainID: 1, Offset: 2, Height: 100}); err != nil {
			t.Errorf("SaveRawCheckpoint() error = %v", err)
		}
	}()
	trades, ok, err := NewHistoryRepo(db, 10*time.Second).RawTrades(ctx, 1, []string{token}, 100, 100)
	if err != nil || !ok || len(trades) != 1 {
		t.Fatalf("RawTrades() = %d trades, %v, %v; want 1 after waiting", len(trades), ok, err)
	}
}

func TestHistoryReadsStoredPrices(t *testing.T) {
	ctx := context.Background()
	db, _ := openClickHouse(t, chdb.MarketSchema, chdb.RawSchema)
	output := mustOutput(t, db)
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
	prices, err := NewHistoryRepo(db, 0).PriceHistory(ctx, 1, []string{"0xweth", "0xnone"}, 100, 105)
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
