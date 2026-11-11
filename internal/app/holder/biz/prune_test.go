package biz

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// emptyRepo stores nothing: every block sees no known holdings.
type emptyRepo struct{}

func (emptyRepo) LoadHolderCheckpoint(context.Context, uint64) (chainsync.Checkpoint, int64, bool, error) {
	return chainsync.Checkpoint{}, 0, false, nil
}

func (emptyRepo) Holdings(context.Context, uint64, []model.HolderKey) ([]model.Holding, error) {
	return nil, nil
}

func (emptyRepo) StaleHoldings(context.Context, uint64, int, string) ([]model.Holding, error) {
	return nil, nil
}

func (emptyRepo) CommitHolders(context.Context, chainsync.Checkpoint, model.HolderChange) error {
	return nil
}

// zeroBalances answers every balanceOf with zero.
type zeroBalances struct{}

func (zeroBalances) Balances(_ context.Context, block rpc.Block, queries []rpc.BalanceQuery) ([]rpc.Balance, error) {
	balances := make([]rpc.Balance, len(queries))
	for index := range balances {
		balances[index] = rpc.Balance{Amount: new(big.Int), Height: block.Height}
	}
	return balances, nil
}

func TestPruningKeepsAtMostTheDepth(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		finalized int64
		// lowest is the lowest rollback height still accepted.
		lowest int64
	}{
		{"finalized unknown", 0, 96},
		{"finalized lags less than the depth", 98, 98},
		{"finalized lags more than the depth", 90, 96},
	} {
		indexer, err := New(Options{ChainID: 1, Repo: emptyRepo{}, Balances: zeroBalances{}, HistoryDepth: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := indexer.Apply(ctx, &streamv1.BlockEvent{BlockHeight: 100, FinalizedHeight: test.finalized}); err != nil {
			t.Fatal(err)
		}
		if err := indexer.Revert(ctx, test.lowest); err != nil {
			t.Errorf("%s: Revert(%d) error = %v", test.name, test.lowest, err)
		}
		if err := indexer.Revert(ctx, test.lowest-1); !errors.Is(err, ErrReorgTooDeep) {
			t.Errorf("%s: Revert(%d) error = %v, want ErrReorgTooDeep", test.name, test.lowest-1, err)
		}
	}
}
