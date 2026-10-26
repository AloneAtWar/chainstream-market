package data

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

const (
	token = "0x1111111111111111111111111111111111111111"
	alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// openRepo connects to CLICKHOUSE_ADDR in a fresh database with the raw
// tables, or skips when it is unset.
func openRepo(t *testing.T) *rawRepo {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	options := chdb.Options{
		Addr:     strings.Split(addr, ","),
		Database: fmt.Sprintf("raw_test_%d", time.Now().UnixNano()),
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
	if err := chdb.Migrate(context.Background(), db, chdb.RawSchema); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return &rawRepo{db: db}
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

func count(t *testing.T, repo *rawRepo, table string) uint64 {
	t.Helper()
	var rows uint64
	if err := repo.db.WithContext(context.Background()).Raw("SELECT count() FROM " + table).Row().Scan(&rows); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return rows
}

func assertBlocks(t *testing.T, repo *rawRepo, want uint64) {
	t.Helper()
	for _, table := range chdb.RawTables {
		perBlock := uint64(1)
		if table == "raw_transfers" {
			perBlock = 2
		}
		if got := count(t, repo, table); got != want*perBlock {
			t.Fatalf("%s rows = %d, want %d", table, got, want*perBlock)
		}
	}
}

func TestStoreWritesDeletesAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	repo := openRepo(t)
	if _, ok, err := repo.LoadRawCheckpoint(ctx, 1); err != nil || ok {
		t.Fatalf("empty LoadRawCheckpoint() = %v, %v", ok, err)
	}
	if err := repo.WriteRaw(ctx, []model.RawBlock{testBlock(100), testBlock(101), {ChainID: 1, Height: 102}}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	assertBlocks(t, repo, 2)

	// A replay deletes the height and everything above, then inserts again.
	if err := repo.DeleteRawAbove(ctx, 1, 100); err != nil {
		t.Fatalf("DeleteRawAbove() error = %v", err)
	}
	assertBlocks(t, repo, 1)
	if err := repo.WriteRaw(ctx, []model.RawBlock{testBlock(101)}); err != nil {
		t.Fatalf("replay WriteRaw() error = %v", err)
	}
	assertBlocks(t, repo, 2)

	var amounts []string
	var bin *uint32
	if err := repo.db.WithContext(ctx).Raw("SELECT amount_raw, active_bin_id FROM raw_trades WHERE block_height = 101").Row().Scan(&amounts, &bin); err != nil {
		t.Fatalf("read trade: %v", err)
	}
	if strings.Join(amounts, ",") != "-100,5" || bin == nil || *bin != 8388608 {
		t.Fatalf("trade amounts = %v, bin = %v", amounts, bin)
	}
	var answer string
	if err := repo.db.WithContext(ctx).Raw("SELECT toString(answer) FROM oracle_updates WHERE block_height = 101").Row().Scan(&answer); err != nil || answer != "-5" {
		t.Fatalf("oracle answer = %q, %v", answer, err)
	}

	for offset, height := range []int64{100, 101} {
		checkpoint := chainsync.Checkpoint{ChainID: 1, Offset: int64(offset), Height: height, Hash: fmt.Sprintf("0x%d", height)}
		if err := repo.SaveRawCheckpoint(ctx, checkpoint); err != nil {
			t.Fatalf("SaveRawCheckpoint() error = %v", err)
		}
	}
	checkpoint, ok, err := repo.LoadRawCheckpoint(ctx, 1)
	if err != nil || !ok || checkpoint.Offset != 1 || checkpoint.Height != 101 || checkpoint.Hash != "0x101" {
		t.Fatalf("LoadRawCheckpoint() = %+v, %v, %v", checkpoint, ok, err)
	}
}

// Old transfers and trades are dropped a month at a time; asset and pool
// facts stay.
func TestRetentionDropsOldFacts(t *testing.T) {
	ctx := context.Background()
	repo := openRepo(t)
	retention := chdb.Retention{RawTransfersDays: 30, RawTradesDays: 30}
	if err := chdb.ApplyTTL(ctx, repo.db, retention, retention.RawTTL()); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	recent := testBlock(200)
	recent.BlockTime = time.Now().UTC()
	if err := repo.WriteRaw(ctx, []model.RawBlock{testBlock(100), recent}); err != nil {
		t.Fatalf("WriteRaw() error = %v", err)
	}
	for _, table := range chdb.RawTables {
		if err := repo.db.WithContext(ctx).Exec("OPTIMIZE TABLE " + table + " FINAL").Error; err != nil {
			t.Fatalf("optimize %s: %v", table, err)
		}
	}
	want := map[string]uint64{"raw_transfers": 2, "raw_trades": 1, "oracle_updates": 2, "asset_facts": 2, "pool_facts": 2}
	for table, rows := range want {
		if got := count(t, repo, table); got != rows {
			t.Errorf("%s rows = %d, want %d", table, got, rows)
		}
	}
}
