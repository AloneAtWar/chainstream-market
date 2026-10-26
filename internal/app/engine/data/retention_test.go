package data

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

func outputAt(height int64, closePrice string, blockTime time.Time) model.BlockOutput {
	output := testOutput(height, closePrice)
	output.BlockTime = blockTime
	output.Trades[0].BlockTime = blockTime
	output.Prices[0].BlockTime = blockTime
	output.Candles[0].OpenTime = blockTime.Truncate(time.Hour)
	minute := output.Candles[0]
	minute.Interval, minute.OpenTime = time.Minute, blockTime.Truncate(time.Minute)
	output.Candles = append(output.Candles, minute)
	return output
}

func optimize(t *testing.T, store *outputRepo, tables ...string) {
	t.Helper()
	for _, table := range tables {
		if err := store.db.WithContext(context.Background()).Exec("OPTIMIZE TABLE " + table + " FINAL").Error; err != nil {
			t.Fatalf("optimize %s: %v", table, err)
		}
	}
}

func TestRetentionAgesRowsOut(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	retention := chdb.Retention{
		TradesDays: 30, TokenPriceRollupDays: 30, CandleCompactDays: 3,
		CandleDays: map[time.Duration]int{time.Minute: 30},
	}
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	old := time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	for _, block := range []model.BlockOutput{
		outputAt(100, "0.03", old), outputAt(101, "0.06", old.Add(12*time.Second)), outputAt(200, "0.09", now),
	} {
		if err := store.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock(%d) error = %v", block.Height, err)
		}
	}
	optimize(t, store, "trades", "token_prices", "candles")

	// Old trades are gone, blocks stay.
	if got := count(t, store, "trades"); got != 1 {
		t.Fatalf("trades = %d, want 1", got)
	}
	if got := count(t, store, "blocks"); got != 3 {
		t.Fatalf("blocks = %d, want 3", got)
	}

	// Old prices rolled up into their latest row.
	var height int64
	var price decimal.Decimal
	if err := store.db.WithContext(ctx).Raw(`SELECT block_height, price_usd FROM token_prices
		WHERE block_time < ?`, now.Add(-time.Hour)).Row().Scan(&height, &price); err != nil {
		t.Fatalf("read old price: %v", err)
	}
	if got := count(t, store, "token_prices"); got != 2 || height != 101 {
		t.Fatalf("token_prices = %d, old height = %d; want 2 rows, 101", got, height)
	}
	assertDecimalEqual(t, price, "0.06")

	// The old 1h bucket keeps its last version; the old 1m bucket is gone.
	var trades uint32
	var closePrice decimal.Decimal
	if err := store.db.WithContext(ctx).Raw(`SELECT update_height, close, trades FROM candles
		WHERE open_time < ? AND interval_seconds = 3600`, now.Add(-24*time.Hour)).Row().Scan(&height, &closePrice, &trades); err != nil {
		t.Fatalf("read old candle: %v", err)
	}
	if height != 101 || trades != 2 {
		t.Fatalf("old candle update_height = %d, trades = %d; want 101, 2", height, trades)
	}
	assertDecimalEqual(t, closePrice, "0.06")
	if got := count(t, store, "candles"); got != 3 {
		t.Fatalf("candles = %d, want old 1h plus recent 1h and 1m", got)
	}

	// The recent block still rewinds.
	if err := store.Rewind(ctx, 1, 150); err != nil {
		t.Fatalf("Rewind() error = %v", err)
	}
	if got := count(t, store, "trades"); got != 0 {
		t.Fatalf("trades after rewind = %d, want 0", got)
	}
}

func createQuery(t *testing.T, store *outputRepo, table string) string {
	t.Helper()
	var create string
	if err := store.db.WithContext(context.Background()).Raw(`SELECT create_table_query FROM system.tables
		WHERE database = currentDatabase() AND name = ?`, table).Row().Scan(&create); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return create
}

func TestApplyRetentionChangesOnlyWhatDiffers(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	mutations := func() uint64 {
		var rows uint64
		if err := store.db.WithContext(ctx).Raw(
			"SELECT count() FROM system.mutations WHERE database = currentDatabase()").Row().Scan(&rows); err != nil {
			t.Fatalf("count mutations: %v", err)
		}
		return rows
	}
	retention := chdb.Retention{
		TradesDays: 90, TokenPriceRollupDays: 30, CandleCompactDays: 3,
		CandleDays: map[time.Duration]int{time.Minute: 30, 5 * time.Minute: 90},
	}
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	applied := mutations()
	if applied == 0 {
		t.Fatal("setting TTLs started no TTL rewrite")
	}
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("second ApplyRetention() error = %v", err)
	}
	if got := mutations(); got != applied {
		t.Fatalf("mutations after reapplying = %d, want %d", got, applied)
	}

	retention.TradesDays = 120
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("changed ApplyRetention() error = %v", err)
	}
	if got := mutations(); got != applied+1 {
		t.Fatalf("mutations after changing trades = %d, want %d", got, applied+1)
	}
	if create := createQuery(t, store, "trades"); !strings.Contains(create, "toIntervalDay(120)") {
		t.Fatalf("trades = %s", create)
	}

	if err := applyRetention(ctx, store.db, chdb.Retention{}); err != nil {
		t.Fatalf("empty ApplyRetention() error = %v", err)
	}
	for _, table := range []string{"trades", "token_prices", "candles"} {
		if create := createQuery(t, store, table); strings.Contains(create, " TTL ") {
			t.Fatalf("%s kept its TTL: %s", table, create)
		}
	}
}

// TestRetentionMovesToCold needs a storage policy with a "default" volume
// and another one, as the server config documents.
func TestRetentionMovesToCold(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	var policy, volume, disk string
	lookupErr := store.db.WithContext(ctx).Raw(`SELECT policy_name, volume_name, disks[1] FROM system.storage_policies
		WHERE volume_name != 'default' AND policy_name IN (
			SELECT policy_name FROM system.storage_policies WHERE volume_name = 'default')
		LIMIT 1`).Row().Scan(&policy, &volume, &disk)
	if lookupErr != nil {
		t.Skipf("no storage policy with a default and a cold volume: %v", lookupErr)
	}
	if err := applyRetention(ctx, store.db, chdb.Retention{
		StoragePolicy: "bad_policy", ColdVolume: volume, ColdAfterDays: 30,
	}); err == nil {
		t.Fatal("ApplyRetention() with an unknown policy error = nil")
	}
	retention := chdb.Retention{StoragePolicy: policy, ColdVolume: volume, ColdAfterDays: 30, TradesDays: 3650}
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	for _, block := range []model.BlockOutput{
		outputAt(100, "0.03", time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC)), outputAt(200, "0.09", time.Now().UTC()),
	} {
		if err := store.WriteBlock(ctx, block); err != nil {
			t.Fatalf("WriteBlock(%d) error = %v", block.Height, err)
		}
	}
	optimize(t, store, "trades")
	rows, err := store.db.WithContext(ctx).Raw("SELECT block_height, _disk_name FROM trades").Rows()
	if err != nil {
		t.Fatalf("read disks: %v", err)
	}
	defer rows.Close() //nolint:errcheck // rows.Err reports read failures
	got := map[int64]string{}
	for rows.Next() {
		var height int64
		var partDisk string
		if err := rows.Scan(&height, &partDisk); err != nil {
			t.Fatalf("scan disk: %v", err)
		}
		got[height] = partDisk
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read disks: %v", err)
	}
	if got[100] != disk || got[200] != "default" {
		t.Fatalf("trade disks = %v, want 100 on %s and 200 on default", got, disk)
	}
	if err := applyRetention(ctx, store.db, retention); err != nil {
		t.Fatalf("second ApplyRetention() error = %v", err)
	}
}
