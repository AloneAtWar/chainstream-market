package data

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

const (
	token = "0x1111111111111111111111111111111111111111"
	alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// openClickHouse connects to CLICKHOUSE_ADDR in a fresh database with the
// market and raw tables, or skips when it is unset.
func openClickHouse(t *testing.T) *gorm.DB {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chdb.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("api_test_%d", time.Now().UnixNano()),
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	db, err := chdb.Connect(context.Background(), options)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Exec("DROP DATABASE IF EXISTS " + options.Database).Error; err != nil {
			t.Errorf("drop database: %v", err)
		}
		if err := chdb.Close(db); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	for _, schema := range [][]string{chdb.MarketSchema, chdb.RawSchema} {
		if err := chdb.Migrate(context.Background(), db, schema); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
	}
	return db
}

func testBlock(height int64) model.RawBlock {
	bin := uint32(8388608)
	return model.RawBlock{
		ChainID: 1, Height: height, BlockTime: time.Unix(1_700_000_000+height*12, 0).UTC(),
		Transfers: []model.RawTransfer{
			{TransactionHash: "0xt1", LogIndex: 1, Token: token, From: alice, To: bob, Amount: big.NewInt(height)},
			{TransactionHash: "0xt2", LogIndex: 2, Token: token, From: bob, To: alice, Amount: new(big.Int).Lsh(big.NewInt(1), 255)},
		},
		Trades: []model.RawTrade{{
			TransactionHash: "0xt3", LogIndex: 3, Protocol: "uniswap_v3", PoolID: "0xpool", Action: "swap",
			Amounts: []model.RawAmount{
				{Asset: token, Raw: "-100", Value: "-0.0001", Decimals: 6, DecimalsKnown: true},
				{Asset: "0xweth", Raw: "5"},
			},
			SqrtPriceX96: "79228162514264337593543950336", Liquidity: "1", Tick: "0", ActiveBinID: &bin,
		}},
		Oracles: []model.RawOracleUpdate{{
			LogIndex: 4, Feed: "0xfeed", Answer: big.NewInt(-5), RoundID: big.NewInt(7), UpdatedAt: 1_700_000_000,
		}},
		Assets: []model.RawAssetFact{{
			Address: token, Standard: "erc20", Recognition: "recognized", MetadataComplete: true, Type: "created",
			SourceHeight: height, DeploymentHeight: height, MetadataJSON: `{"symbol":"TKN","decimals":6}`,
		}},
		Pools: []model.RawPoolFact{{
			PoolID: "0xpool", Protocol: "uniswap_v3", TokenAddresses: []string{token, "0xweth"},
			SourceHeight: height, Created: true,
		}},
	}
}

func TestTransfersPagesNewestFirst(t *testing.T) {
	ctx := context.Background()
	db := openClickHouse(t)
	store := &transferReader{db: db}
	if err := chdb.WriteRaw(ctx, db, []model.RawBlock{testBlock(100), testBlock(101)}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	other := testBlock(102)
	other.Transfers = []model.RawTransfer{{LogIndex: 0, Token: "0xother", From: alice, To: "0xcc", Amount: big.NewInt(1)}}
	if err := chdb.WriteRaw(ctx, db, []model.RawBlock{other}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}

	page := func(query model.TransferQuery) string {
		t.Helper()
		query.ChainID = 1
		transfers, err := store.Transfers(ctx, query)
		if err != nil {
			t.Fatalf("Transfers(%+v) error = %v", query, err)
		}
		keys := make([]string, len(transfers))
		for index, transfer := range transfers {
			keys[index] = fmt.Sprintf("%d/%d", transfer.BlockHeight, transfer.LogIndex)
		}
		return strings.Join(keys, " ")
	}
	if got := page(model.TransferQuery{Token: token, Limit: 3}); got != "101/2 101/1 100/2" {
		t.Fatalf("token page = %s", got)
	}
	one := uint32(1)
	if got := page(model.TransferQuery{Token: token, BeforeHeight: 101, BeforeLogIndex: &one, Limit: 10}); got != "100/2 100/1" {
		t.Fatalf("token page after 101/1 = %s", got)
	}
	if got := page(model.TransferQuery{Token: token, BeforeHeight: 101, Limit: 10}); got != "100/2 100/1" {
		t.Fatalf("token page before 101 = %s", got)
	}
	if got := page(model.TransferQuery{Holder: alice, Limit: 10}); got != "102/0 101/2 101/1 100/2 100/1" {
		t.Fatalf("holder page = %s", got)
	}
	if got := page(model.TransferQuery{Holder: "0xcc", Token: "0xother", Limit: 10}); got != "102/0" {
		t.Fatalf("holder and token page = %s", got)
	}

	transfers, err := store.Transfers(ctx, model.TransferQuery{ChainID: 1, Token: token, Limit: 1})
	if err != nil || len(transfers) != 1 {
		t.Fatalf("Transfers() = %v, %v", transfers, err)
	}
	if want := new(big.Int).Lsh(big.NewInt(1), 255); transfers[0].Amount.Cmp(want) != 0 || transfers[0].From != bob {
		t.Fatalf("transfer = %+v", transfers[0])
	}
	if _, err := store.Transfers(ctx, model.TransferQuery{ChainID: 1, Limit: 1}); err == nil {
		t.Fatal("Transfers() without token or holder error = nil")
	}
}

// The holder filter must be the index expression, or every granule is read.
func TestHolderFilterUsesPartyIndex(t *testing.T) {
	ctx := context.Background()
	db := openClickHouse(t)
	if err := chdb.WriteRaw(ctx, db, []model.RawBlock{testBlock(100)}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	rows, err := db.WithContext(ctx).Raw(
		"EXPLAIN indexes = 1 SELECT count() FROM raw_transfers WHERE chain_id = 1 AND has([from_address, to_address], ?)", alice).Rows()
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain: %v", err)
		}
		plan.WriteString(line + "\n")
	}
	if !strings.Contains(plan.String(), "raw_transfers_party") {
		t.Fatalf("plan does not use raw_transfers_party:\n%s", plan.String())
	}
}
