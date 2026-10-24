package pgstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/storagetest"
)

// openTestStore connects to POSTGRES_DSN in a fresh schema, or skips when it
// is unset.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	schema := fmt.Sprintf("market_test_%d", time.Now().UnixNano())
	store, err := Open(context.Background(), Options{DSN: dsn, Schema: schema})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if _, err := store.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		store.Close()
	})
	return store
}

func TestStateStore(t *testing.T) {
	storagetest.StateStore(t, openTestStore(t))
}

func TestCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if _, ok, err := store.Load(ctx, 1); err != nil || ok {
		t.Fatalf("Load() on empty store = %v, %v", ok, err)
	}
	want := storage.Checkpoint{ChainID: 1, Offset: 42, Height: 100, Hash: "0xabc", UpdatedAt: time.UnixMilli(1_700_000_000_123).UTC()}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, ok, err := store.Load(ctx, 1)
	if err != nil || !ok || got != want {
		t.Fatalf("Load() = %+v, %v, %v; want %+v", got, ok, err, want)
	}
}

func TestOpenRejectsBadOptions(t *testing.T) {
	if _, err := Open(context.Background(), Options{}); err == nil {
		t.Fatal("Open() without dsn error = nil")
	}
	if _, err := Open(context.Background(), Options{DSN: "postgres://x", Schema: "bad;name"}); err == nil {
		t.Fatal("Open() with bad schema error = nil")
	}
}

// TestOpenConcurrent opens many stores on one empty schema at once, as
// several services starting together do. Without the migration lock,
// concurrent CREATE ... IF NOT EXISTS fails with a duplicate key error.
func TestOpenConcurrent(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("market_test_%d", time.Now().UnixNano())
	const services = 8
	stores := make(chan *Store, services)
	errs := make(chan error, services)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range services {
		wg.Go(func() {
			<-start
			store, err := Open(ctx, Options{DSN: dsn, Schema: schema})
			if err != nil {
				errs <- err
				return
			}
			stores <- store
		})
	}
	close(start)
	wg.Wait()
	close(stores)
	close(errs)
	var opened []*Store
	for store := range stores {
		opened = append(opened, store)
	}
	t.Cleanup(func() {
		if len(opened) > 0 {
			if _, err := opened[0].pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
				t.Errorf("drop schema: %v", err)
			}
		}
		for _, store := range opened {
			store.Close()
		}
	})
	for err := range errs {
		t.Errorf("concurrent Open() error = %v", err)
	}
	if len(opened) != services {
		t.Fatalf("opened %d stores, want %d", len(opened), services)
	}
	if err := opened[0].Save(ctx, storage.Checkpoint{ChainID: 1, Height: 1, Hash: "0x1", UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("Save() after concurrent Open() error = %v", err)
	}
}
