package storage_test

import (
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/storagetest"
)

func TestMemoryStateStore(t *testing.T) {
	storagetest.StateStore(t, storage.NewMemory())
}
