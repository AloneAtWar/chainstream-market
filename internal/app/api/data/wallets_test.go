package data

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/shopspring/decimal"
)

func TestWalletReader(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	market := openClickHouse(t)
	ctx := context.Background()
	if err := chdb.Migrate(ctx, market, chdb.WalletSchema); err != nil {
		t.Fatal(err)
	}
	state, err := pgdb.Open(ctx, pgdb.Options{DSN: dsn, Schema: fmt.Sprintf("api_wallet_test_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pgdb.Close(state) })
	data := &Data{market: market, state: state}
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	// 持仓：一个仍持有、有现价和符号，一个已清仓。
	positions, err := pgdb.MarshalPositions(1, []model.Position{
		{Wallet: "0xaa", Token: "0x01", Quantity: decimal.RequireFromString("10"), CostUSD: decimal.RequireFromString("20"),
			RealizedUSD: decimal.RequireFromString("5"), LastTradeTime: at.Add(time.Hour), OpenedAt: at},
		{Wallet: "0xaa", Token: "0x02", RealizedUSD: decimal.RequireFromString("-3"), LastTradeTime: at},
		{Wallet: "0xbb", Token: "0x01", RealizedUSD: decimal.RequireFromString("50"), LastTradeTime: at},
	})
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`INSERT INTO wallet_positions SELECT * FROM jsonb_populate_recordset(NULL::wallet_positions, CAST(? AS jsonb))`,
		`INSERT INTO token_rankings (chain_id, token, price_usd) VALUES (1, '0x01', 3)`,
		`INSERT INTO registry_tokens (chain_id, address, standard, recognition, metadata_complete, symbol, created, source_height, first_height, updated_height) VALUES (1, '0x01', 'erc20', 'recognized', true, 'ONE', false, 1, 1, 1)`,
		`INSERT INTO wallet_labels (chain_id, wallet, label, source, since) VALUES (1, '0xbb', 'smart_money', 'rule', now())`,
	}
	for index, statement := range statements {
		args := []any{}
		if index == 0 {
			args = append(args, positions)
		}
		if err = state.Exec(statement, args...).Error; err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	reader := NewWalletReader(data)
	open := true
	held, err := reader.Positions(ctx, model.PositionQuery{ChainID: 1, Wallet: "0xaa", Open: &open, Limit: 10})
	if err != nil || len(held) != 1 || held[0].Symbol == nil || *held[0].Symbol != "ONE" || held[0].UnrealizedUSD().String() != "10" {
		t.Fatalf("open positions %+v %v", held, err)
	}
	traders, err := reader.Positions(ctx, model.PositionQuery{ChainID: 1, Token: "0x01", Limit: 10})
	if err != nil || len(traders) != 2 || traders[0].Wallet != "0xbb" {
		t.Fatalf("traders %+v %v", traders, err)
	}

	// 统计：只读最新一次汇总的版本。版本是毫秒时间戳，wallet_stats 按它过期。
	v1 := uint64(time.Now().UnixMilli())
	v2 := v1 + 1
	stats := []chdb.WalletStatsRow{
		{ChainID: 1, WindowDays: 30, Wallet: "0xaa", RealizedUSD: decimal.RequireFromString("100"), Wins: 1, Counted: 4, Version: v1},
		{ChainID: 1, WindowDays: 30, Wallet: "0xaa", RealizedUSD: decimal.RequireFromString("2"), Wins: 1, Counted: 4, Version: v2},
		{ChainID: 1, WindowDays: 30, Wallet: "0xbb", RealizedUSD: decimal.RequireFromString("50"), Wins: 3, Counted: 3, Version: v2},
		{ChainID: 1, WindowDays: 7, Wallet: "0xbb", RealizedUSD: decimal.RequireFromString("9"), Version: v2},
	}
	refreshes := []chdb.WalletRefreshRow{{ChainID: 1, Version: v1, HeadTime: at}, {ChainID: 1, Version: v2, HeadTime: at}}
	if err = chdb.Insert(ctx, market, &stats); err != nil {
		t.Fatal(err)
	}
	if err = chdb.Insert(ctx, market, &refreshes); err != nil {
		t.Fatal(err)
	}
	ranked, err := reader.WalletStats(ctx, model.WalletStatsQuery{ChainID: 1, WindowDays: 30, Sort: "winrate", Limit: 10})
	if err != nil || len(ranked) != 2 || ranked[0].Wallet != "0xbb" || !ranked[1].RealizedUSD.Equal(decimal.RequireFromString("2")) {
		t.Fatalf("ranked %+v %v", ranked, err)
	}
	profile, err := reader.WalletStats(ctx, model.WalletStatsQuery{ChainID: 1, Wallets: []string{"0xbb"}})
	if err != nil || len(profile) != 2 {
		t.Fatalf("profile %+v %v", profile, err)
	}

	trades := []model.WalletTrade{
		{ChainID: 1, ApplyHeight: 5, BlockHeight: 5, BlockTime: at, Wallet: "0xaa", Token: "0x01", Side: model.SideBuy, Event: model.WalletEventOpen},
		{ChainID: 1, ApplyHeight: 6, BlockHeight: 6, BlockTime: at.Add(time.Minute), Wallet: "0xaa", Token: "0x02", Side: model.SideSell, Event: model.WalletEventUntracked},
	}
	if err = chdb.InsertWalletTrades(ctx, market, trades); err != nil {
		t.Fatal(err)
	}
	got, err := reader.WalletTrades(ctx, model.WalletTradeQuery{ChainID: 1, Wallet: "0xaa", BeforeHeight: 6, Limit: 10})
	if err != nil || len(got) != 1 || got[0].Event != model.WalletEventOpen {
		t.Fatalf("trades %+v %v", got, err)
	}

	cache, stop := NewLabelCache(ctx, data, &conf.Bootstrap{Chains: []*config.Chain{{ChainId: 1}}}, nil)
	defer stop()
	if labels := cache.WalletLabels(1, "0xbb"); len(labels) != 1 || labels[0] != model.LabelSmartMoney {
		t.Fatalf("cached labels %v", labels)
	}
}
