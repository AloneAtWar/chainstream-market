package biz_test

import (
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz/biztest"
)

func TestMemoryStateStore(t *testing.T) {
	biztest.StateStore(t, biz.NewMemory())
}
