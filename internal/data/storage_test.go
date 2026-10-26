package data

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// The roles each driver's backend has to serve for the commands that need
// it.
var (
	_ interface {
		storage.Output
		storage.Reader
		storage.RankingSource
		retainer
	} = (*chstore.Store)(nil)
	_ interface {
		storage.RawStore
		storage.TransferReader
		historian
		retainer
	} = (*chraw.Store)(nil)
	_ interface {
		storage.StateStore
		storage.HolderStore
		storage.HolderReader
		storage.RegistryStore
		storage.RegistryReader
		storage.RankingStore
		storage.RankingReader
	} = (*pgstore.Store)(nil)
	_ storage.Output = (*storage.Memory)(nil)
)

func TestMemoryMarketBackend(t *testing.T) {
	ctx := context.Background()
	config := &conf.Storage{Driver: conf.StorageMemory}
	backend, cleanup, err := NewMarketBackend(ctx, config, slog.Default())
	if err != nil {
		t.Fatalf("NewMarketBackend() error = %v", err)
	}
	defer cleanup()
	output, err := NewOutput(ctx, backend, config)
	if err != nil || output == nil {
		t.Fatalf("NewOutput() = %v, %v, want the memory store", output, err)
	}
	_, err = NewReader(backend)
	if err == nil || err.Error() != `storage.driver "memory" does not provide storage.Reader` {
		t.Fatalf("NewReader() error = %v", err)
	}
	if _, err := NewRankingSource(backend); err == nil {
		t.Fatal("NewRankingSource() error = nil, want the memory store to serve no rankings")
	}
}

func TestMemoryHasNoRawBackend(t *testing.T) {
	config := &conf.Storage{Driver: conf.StorageMemory}
	_, _, err := NewRawBackend(context.Background(), config, slog.Default())
	if err == nil || !strings.Contains(err.Error(), `storage.driver "memory" has no raw backend; use one of: clickhouse`) {
		t.Fatalf("NewRawBackend() error = %v", err)
	}
	history, cleanup, err := NewHistory(context.Background(), config, &conf.Market{BackfillBlocks: 100}, slog.Default())
	if err != nil || history != nil {
		t.Fatalf("NewHistory() = %v, %v, want no history", history, err)
	}
	cleanup()
}

func TestMemoryStateBackend(t *testing.T) {
	backend, cleanup, err := NewStateBackend(context.Background(), &conf.State{Driver: conf.StateMemory}, slog.Default())
	if err != nil {
		t.Fatalf("NewStateBackend() error = %v", err)
	}
	defer cleanup()
	if store := NewStateStore(backend); store != nil {
		t.Fatalf("NewStateStore() = %v, want nil", store)
	}
	if readers := NewStateReaders(backend); readers != (StateReaders{}) {
		t.Fatalf("NewStateReaders() = %+v, want none", readers)
	}
	for name, provide := range map[string]func(*StateBackend) error{
		"storage.HolderStore":   func(backend *StateBackend) error { _, err := NewHolderStore(backend); return err },
		"storage.RegistryStore": func(backend *StateBackend) error { _, err := NewRegistryStore(backend); return err },
		"storage.RankingStore":  func(backend *StateBackend) error { _, err := NewRankingStore(backend); return err },
	} {
		if err := provide(backend); err == nil || err.Error() != `state.driver "memory" does not provide `+name {
			t.Errorf("%s error = %v", name, err)
		}
	}
}

func TestUnknownDriver(t *testing.T) {
	_, _, err := NewStateBackend(context.Background(), &conf.State{Driver: "sqlite"}, slog.Default())
	if err == nil || err.Error() != `state.driver "sqlite" has no state backend; use one of: memory, postgres` {
		t.Fatalf("NewStateBackend() error = %v", err)
	}
}
