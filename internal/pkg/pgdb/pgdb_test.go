package pgdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"gorm.io/gorm"
)

func TestOpenRejectsBadOptions(t *testing.T) {
	if _, err := Open(context.Background(), Options{}); err == nil {
		t.Fatal("Open() without dsn error = nil")
	}
	if _, err := Open(context.Background(), Options{DSN: "postgres://x", Schema: "bad;name"}); err == nil {
		t.Fatal("Open() with bad schema error = nil")
	}
}

// TestOpenConcurrent opens many connections on one empty schema at once, as
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
	dbs := make(chan *gorm.DB, services)
	errs := make(chan error, services)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range services {
		wg.Go(func() {
			<-start
			db, err := Open(ctx, Options{DSN: dsn, Schema: schema})
			if err != nil {
				errs <- err
				return
			}
			dbs <- db
		})
	}
	close(start)
	wg.Wait()
	close(dbs)
	close(errs)
	var opened []*gorm.DB
	for db := range dbs {
		opened = append(opened, db)
	}
	t.Cleanup(func() {
		if len(opened) > 0 {
			if err := opened[0].WithContext(ctx).Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE").Error; err != nil {
				t.Errorf("drop schema: %v", err)
			}
		}
		for _, db := range opened {
			Close(db)
		}
	})
	for err := range errs {
		t.Errorf("concurrent Open() error = %v", err)
	}
	if len(opened) != services {
		t.Fatalf("opened %d connections, want %d", len(opened), services)
	}
	if err := SaveCheckpoint(opened[0].WithContext(ctx), EngineCheckpoints,
		chainsync.Checkpoint{ChainID: 1, Height: 1, Hash: "0x1", UpdatedAt: time.Now().UTC()}, 0); err != nil {
		t.Fatalf("SaveCheckpoint() after concurrent Open() error = %v", err)
	}
}
