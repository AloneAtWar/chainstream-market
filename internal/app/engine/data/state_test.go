package data

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz/biztest"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
)

// openTestStore connects to POSTGRES_DSN in a fresh schema, or skips when it
// is unset.
func openTestState(t *testing.T) *stateRepo {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	schema := fmt.Sprintf("market_test_%d", time.Now().UnixNano())
	db, err := pgdb.Open(context.Background(), pgdb.Options{DSN: dsn, Schema: schema})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE").Error; err != nil {
			t.Errorf("drop schema: %v", err)
		}
		pgdb.Close(db)
	})
	return &stateRepo{db: db}
}

func TestStateStore(t *testing.T) {
	biztest.StateStore(t, openTestState(t))
}

func TestCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTestState(t)
	if _, ok, err := store.Load(ctx, 1); err != nil || ok {
		t.Fatalf("Load() on empty store = %v, %v", ok, err)
	}
	want := chainsync.Checkpoint{ChainID: 1, Offset: 42, Height: 100, Hash: "0xabc", UpdatedAt: time.UnixMilli(1_700_000_000_123).UTC()}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, ok, err := store.Load(ctx, 1)
	if err != nil || !ok || got != want {
		t.Fatalf("Load() = %+v, %v, %v; want %+v", got, ok, err, want)
	}
}
