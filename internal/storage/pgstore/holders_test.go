package pgstore

import (
	"context"
	"math/big"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
)

func holderDelta(token, holder string, delta int64) model.HolderDelta {
	return model.HolderDelta{HolderKey: model.HolderKey{Token: token, Holder: holder}, Delta: big.NewInt(delta)}
}

func holderSeed(token, holder string, balance int64) model.HolderSeed {
	return model.HolderSeed{Holding: model.Holding{HolderKey: model.HolderKey{Token: token, Holder: holder}, Balance: big.NewInt(balance)}}
}

func commitHolders(t *testing.T, store *Store, change model.HolderChange) {
	t.Helper()
	height := change.Height
	if change.Reverted && len(change.Created)+len(change.Deltas) == 0 {
		height = change.RevertTo
	}
	if err := store.CommitHolders(context.Background(), chainsync.Checkpoint{ChainID: 1, Height: height}, change); err != nil {
		t.Fatalf("CommitHolders() error = %v", err)
	}
}

// assertHolderCounts checks holder_counts against both the expected counts
// and a full count of holder_balances.
func assertHolderCounts(t *testing.T, store *Store, want map[string]int64) {
	t.Helper()
	for token, count := range want {
		got, err := store.HolderCount(context.Background(), 1, token)
		if err != nil || got != count {
			t.Fatalf("HolderCount(%s) = %d, %v; want %d", token, got, err, count)
		}
		var counted int64
		if err := store.db.Raw(`SELECT count(*) FROM holder_balances WHERE chain_id = 1 AND token = ? AND balance > 0`,
			token).Scan(&counted).Error; err != nil {
			t.Fatalf("count holder_balances: %v", err)
		}
		if counted != count {
			t.Fatalf("holder_balances count(%s) = %d, want %d", token, counted, count)
		}
	}
}

func TestHolderCountsFollowZeroCrossings(t *testing.T) {
	store := openTestStore(t)
	const tokenA, tokenB = "0xaa", "0xbb"
	assertHolderCounts(t, store, map[string]int64{tokenA: 0})

	// A zero or negative seed is not a holder.
	commitHolders(t, store, model.HolderChange{Height: 1, Created: []model.HolderSeed{
		holderSeed(tokenA, "0x1", 10), holderSeed(tokenA, "0x2", 0), holderSeed(tokenA, "0x3", -5),
		holderSeed(tokenB, "0x1", 7),
	}})
	assertHolderCounts(t, store, map[string]int64{tokenA: 1, tokenB: 1})

	// 0x1 empties, 0x2 and 0x3 turn positive, and a change that stays
	// positive moves nothing.
	commitHolders(t, store, model.HolderChange{Height: 2, Deltas: []model.HolderDelta{
		holderDelta(tokenA, "0x1", -10), holderDelta(tokenA, "0x2", 4), holderDelta(tokenA, "0x3", 6),
		holderDelta(tokenB, "0x1", 3),
	}})
	assertHolderCounts(t, store, map[string]int64{tokenA: 2, tokenB: 1})

	commitHolders(t, store, model.HolderChange{Height: 3,
		Created: []model.HolderSeed{holderSeed(tokenA, "0x4", 1), holderSeed(tokenB, "0x2", 2)},
		Deltas:  []model.HolderDelta{holderDelta(tokenA, "0x2", -4), holderDelta(tokenB, "0x1", -10)},
	})
	assertHolderCounts(t, store, map[string]int64{tokenA: 2, tokenB: 1})

	// Reverting block 3 deletes its created holdings and restores the
	// balances it emptied.
	commitHolders(t, store, model.HolderChange{Reverted: true, RevertTo: 2})
	assertHolderCounts(t, store, map[string]int64{tokenA: 2, tokenB: 1})

	// Reverting block 2 as well brings back the counts after block 1.
	commitHolders(t, store, model.HolderChange{Reverted: true, RevertTo: 1})
	assertHolderCounts(t, store, map[string]int64{tokenA: 1, tokenB: 1})

	// A replacement block 2 is reverted and applied in one change.
	commitHolders(t, store, model.HolderChange{Reverted: true, RevertTo: 1, Height: 2,
		Deltas: []model.HolderDelta{holderDelta(tokenA, "0x2", 1)},
	})
	balances, err := store.Holdings(context.Background(), 1, []model.HolderKey{{Token: tokenA, Holder: "0x2"}})
	if err != nil || len(balances) != 1 || balances[0].Balance.Int64() != 1 {
		t.Fatalf("Holdings() = %+v, %v", balances, err)
	}
	assertHolderCounts(t, store, map[string]int64{tokenA: 2, tokenB: 1})
}

func TestHolderCountsBackfill(t *testing.T) {
	store := openTestStore(t)
	commitHolders(t, store, model.HolderChange{Height: 1, Created: []model.HolderSeed{
		holderSeed("0xaa", "0x1", 10), holderSeed("0xaa", "0x2", 20), holderSeed("0xaa", "0x3", 0),
	}})
	// A schema from before holder_counts existed is counted on the next
	// migration; later migrations leave the table alone.
	if err := store.db.Exec(`DROP TABLE holder_counts`).Error; err != nil {
		t.Fatalf("drop holder_counts: %v", err)
	}
	var schema string
	if err := store.db.Raw(`SELECT current_schema()`).Scan(&schema).Error; err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	for range 2 {
		if err := pgdb.Migrate(store.db, schema); err != nil {
			t.Fatalf("migrate() error = %v", err)
		}
	}
	assertHolderCounts(t, store, map[string]int64{"0xaa": 2})
}
