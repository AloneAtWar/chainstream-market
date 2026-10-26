package chraw

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
)

const (
	token = "0x1111111111111111111111111111111111111111"
	alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// openTestStore connects to CLICKHOUSE_ADDR in a fresh database, or skips
// when it is unset.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chstore.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("raw_test_%d", time.Now().UnixNano()),
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
	return store
}

func testBlock(height int64) storage.RawBlock {
	bin := uint32(8388608)
	return storage.RawBlock{
		ChainID: 1, Height: height, BlockTime: time.Unix(1_700_000_000+height*12, 0).UTC(),
		Transfers: []storage.RawTransfer{
			{TransactionHash: "0xt1", LogIndex: 1, Token: token, From: alice, To: bob, Amount: big.NewInt(height)},
			{TransactionHash: "0xt2", LogIndex: 2, Token: token, From: bob, To: alice, Amount: new(big.Int).Lsh(big.NewInt(1), 255)},
		},
		Trades: []storage.RawTrade{{
			TransactionHash: "0xt3", LogIndex: 3, Protocol: "uniswap_v3", PoolID: "0xpool", Action: "swap",
			Amounts: []storage.RawAmount{
				{Asset: token, Raw: "-100", Value: "-0.0001", Decimals: 6, DecimalsKnown: true},
				{Asset: "0xweth", Raw: "5"},
			},
			SqrtPriceX96: "79228162514264337593543950336", Liquidity: "1", Tick: "0", ActiveBinID: &bin,
		}},
		Oracles: []storage.RawOracleUpdate{{
			LogIndex: 4, Feed: "0xfeed", Answer: big.NewInt(-5), RoundID: big.NewInt(7), UpdatedAt: 1_700_000_000,
		}},
		Assets: []storage.RawAssetFact{{
			Address: token, Standard: "erc20", Recognition: "recognized", MetadataComplete: true, Type: "created",
			SourceHeight: height, DeploymentHeight: height, MetadataJSON: `{"symbol":"TKN","decimals":6}`,
		}},
		Pools: []storage.RawPoolFact{{
			PoolID: "0xpool", Protocol: "uniswap_v3", TokenAddresses: []string{token, "0xweth"},
			SourceHeight: height, Created: true,
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

func assertBlocks(t *testing.T, store *Store, want uint64) {
	t.Helper()
	for _, table := range tables {
		perBlock := uint64(1)
		if table == "raw_transfers" {
			perBlock = 2
		}
		if got := count(t, store, table); got != want*perBlock {
			t.Fatalf("%s rows = %d, want %d", table, got, want*perBlock)
		}
	}
}

func TestStoreWritesDeletesAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if _, ok, err := store.LoadRawCheckpoint(ctx, 1); err != nil || ok {
		t.Fatalf("empty LoadRawCheckpoint() = %v, %v", ok, err)
	}
	if err := store.WriteRaw(ctx, []storage.RawBlock{testBlock(100), testBlock(101), {ChainID: 1, Height: 102}}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	assertBlocks(t, store, 2)

	// A replay deletes the height and everything above, then inserts again.
	if err := store.DeleteRawAbove(ctx, 1, 100); err != nil {
		t.Fatalf("DeleteRawAbove() error = %v", err)
	}
	assertBlocks(t, store, 1)
	if err := store.WriteRaw(ctx, []storage.RawBlock{testBlock(101)}); err != nil {
		t.Fatalf("replay WriteRaw() error = %v", err)
	}
	assertBlocks(t, store, 2)

	var amounts []string
	var bin *uint32
	if err := store.db.WithContext(ctx).Raw("SELECT amount_raw, active_bin_id FROM raw_trades WHERE block_height = 101").Row().Scan(&amounts, &bin); err != nil {
		t.Fatalf("read trade: %v", err)
	}
	if strings.Join(amounts, ",") != "-100,5" || bin == nil || *bin != 8388608 {
		t.Fatalf("trade amounts = %v, bin = %v", amounts, bin)
	}
	var answer string
	if err := store.db.WithContext(ctx).Raw("SELECT toString(answer) FROM oracle_updates WHERE block_height = 101").Row().Scan(&answer); err != nil || answer != "-5" {
		t.Fatalf("oracle answer = %q, %v", answer, err)
	}

	for offset, height := range []int64{100, 101} {
		checkpoint := storage.Checkpoint{ChainID: 1, Offset: int64(offset), Height: height, Hash: fmt.Sprintf("0x%d", height)}
		if err := store.SaveRawCheckpoint(ctx, checkpoint); err != nil {
			t.Fatalf("SaveRawCheckpoint() error = %v", err)
		}
	}
	checkpoint, ok, err := store.LoadRawCheckpoint(ctx, 1)
	if err != nil || !ok || checkpoint.Offset != 1 || checkpoint.Height != 101 || checkpoint.Hash != "0x101" {
		t.Fatalf("LoadRawCheckpoint() = %+v, %v, %v", checkpoint, ok, err)
	}
}

func TestTransfersPagesNewestFirst(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if err := store.WriteRaw(ctx, []storage.RawBlock{testBlock(100), testBlock(101)}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	other := testBlock(102)
	other.Transfers = []storage.RawTransfer{{LogIndex: 0, Token: "0xother", From: alice, To: "0xcc", Amount: big.NewInt(1)}}
	if err := store.WriteRaw(ctx, []storage.RawBlock{other}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}

	page := func(query storage.TransferQuery) string {
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
	if got := page(storage.TransferQuery{Token: token, Limit: 3}); got != "101/2 101/1 100/2" {
		t.Fatalf("token page = %s", got)
	}
	one := uint32(1)
	if got := page(storage.TransferQuery{Token: token, BeforeHeight: 101, BeforeLogIndex: &one, Limit: 10}); got != "100/2 100/1" {
		t.Fatalf("token page after 101/1 = %s", got)
	}
	if got := page(storage.TransferQuery{Token: token, BeforeHeight: 101, Limit: 10}); got != "100/2 100/1" {
		t.Fatalf("token page before 101 = %s", got)
	}
	if got := page(storage.TransferQuery{Holder: alice, Limit: 10}); got != "102/0 101/2 101/1 100/2 100/1" {
		t.Fatalf("holder page = %s", got)
	}
	if got := page(storage.TransferQuery{Holder: "0xcc", Token: "0xother", Limit: 10}); got != "102/0" {
		t.Fatalf("holder and token page = %s", got)
	}

	transfers, err := store.Transfers(ctx, storage.TransferQuery{ChainID: 1, Token: token, Limit: 1})
	if err != nil || len(transfers) != 1 {
		t.Fatalf("Transfers() = %v, %v", transfers, err)
	}
	if want := new(big.Int).Lsh(big.NewInt(1), 255); transfers[0].Amount.Cmp(want) != 0 || transfers[0].From != bob {
		t.Fatalf("transfer = %+v", transfers[0])
	}
	if _, err := store.Transfers(ctx, storage.TransferQuery{ChainID: 1, Limit: 1}); err == nil {
		t.Fatal("Transfers() without token or holder error = nil")
	}
}

// The holder filter must be the index expression, or every granule is read.
func TestHolderFilterUsesPartyIndex(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if err := store.WriteRaw(ctx, []storage.RawBlock{testBlock(100)}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	rows, err := store.db.WithContext(ctx).Raw(
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

// Old transfers and trades are dropped a month at a time; asset and pool
// facts stay.
func TestRetentionDropsOldFacts(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	retention := chstore.Retention{RawTransfersDays: 30, RawTradesDays: 30}
	if err := store.ApplyRetention(ctx, retention); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	recent := testBlock(200)
	recent.BlockTime = time.Now().UTC()
	if err := store.WriteRaw(ctx, []storage.RawBlock{testBlock(100), recent}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	for _, table := range tables {
		if err := store.db.WithContext(ctx).Exec("OPTIMIZE TABLE " + table + " FINAL").Error; err != nil {
			t.Fatalf("optimize %s: %v", table, err)
		}
	}
	want := map[string]uint64{"raw_transfers": 2, "raw_trades": 1, "oracle_updates": 2, "asset_facts": 2, "pool_facts": 2}
	for table, rows := range want {
		if got := count(t, store, table); got != rows {
			t.Errorf("%s rows = %d, want %d", table, got, rows)
		}
	}
}
