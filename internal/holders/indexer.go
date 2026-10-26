// Package holders tracks ERC-20 balances per holder from a chain topic's
// transfers.
//
// A holding is seeded with balanceOf the first time it appears, read at the
// hash of the block it appears in, so it already includes that block's
// transfers. From then on each block adds its net transfers. Tokens whose
// balances move without a Transfer, such as rebasing or fee-on-transfer
// tokens, drift from the sum of transfers, so every ReconcileEvery blocks
// the least recently verified holdings are checked against balanceOf again
// and corrected.
//
// Balances live only in the store, never in memory, and every change is
// stored as an undoable delta, so a reorg subtracts what the reverted blocks
// added.
package holders

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
var ErrReorgTooDeep = errors.New("holders: reorg deeper than retained history")

const zeroAddress = "0x0000000000000000000000000000000000000000"

// Balances reads balanceOf at a block; *rpc.Client implements it.
type Balances interface {
	BalancesAt(ctx context.Context, blockHash string, queries []rpc.BalanceQuery) ([]*big.Int, error)
}

type Options struct {
	ChainID  uint64
	Store    storage.HolderStore
	Balances Balances
	// HistoryDepth is how many blocks of undo history to keep in addition
	// to everything above the finalized height.
	HistoryDepth int64
	// ReconcileEvery is how often, in blocks, stale holdings are checked
	// against balanceOf. Zero disables reconciliation.
	ReconcileEvery int64
	// ReconcileBatch is how many holdings each reconciliation checks.
	ReconcileBatch int
	Logger         *slog.Logger
}

// Indexer is an chainsync.Handler for one chain. It is not safe for concurrent
// use.
type Indexer struct {
	options       Options
	logger        *slog.Logger
	pending       model.HolderChange
	prunedThrough int64
}

func New(options Options) (*Indexer, error) {
	if options.ChainID == 0 || options.Store == nil || options.Balances == nil {
		return nil, errors.New("holders: chain id, store and balances are required")
	}
	if options.HistoryDepth <= 0 {
		return nil, errors.New("holders: history depth must be positive")
	}
	if options.ReconcileEvery < 0 || (options.ReconcileEvery > 0 && options.ReconcileBatch <= 0) {
		return nil, errors.New("holders: reconciliation needs a positive interval and batch")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Indexer{options: options, logger: logger}, nil
}

func (indexer *Indexer) Restore(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	checkpoint, prunedThrough, ok, err := indexer.options.Store.LoadHolderCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	indexer.prunedThrough = prunedThrough
	return checkpoint, true, nil
}

func (indexer *Indexer) Apply(ctx context.Context, block *streamv1.BlockEvent) error {
	height, hash := block.GetBlockHeight(), block.GetBlockHash()
	deltas := netTransfers(block.GetTransfers())
	keys := make([]model.HolderKey, 0, len(deltas))
	for key := range deltas {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, compareKeys)

	known, err := indexer.options.Store.Holdings(ctx, indexer.options.ChainID, keys)
	if err != nil {
		return fmt.Errorf("load holdings: %w", err)
	}
	existing := make(map[model.HolderKey]struct{}, len(known))
	for _, holding := range known {
		existing[holding.HolderKey] = struct{}{}
	}
	change := model.HolderChange{Height: height}
	var fresh []model.HolderKey
	for _, key := range keys {
		if _, ok := existing[key]; ok {
			if deltas[key].Sign() != 0 {
				change.Deltas = append(change.Deltas, model.HolderDelta{HolderKey: key, Delta: deltas[key]})
			}
			continue
		}
		fresh = append(fresh, key)
	}
	seeds, err := indexer.balances(ctx, hash, fresh)
	if err != nil {
		return fmt.Errorf("seed holdings: %w", err)
	}
	for index, key := range fresh {
		seed := model.HolderSeed{Holding: model.Holding{HolderKey: key, Balance: seeds[index]}, Verified: true}
		if seed.Balance == nil {
			// balanceOf failed; the block's net transfers are the best guess.
			seed.Balance, seed.Verified = deltas[key], false
		}
		change.Created = append(change.Created, seed)
	}

	if every := indexer.options.ReconcileEvery; every > 0 && height%every == 0 {
		corrections, reconcileErr := indexer.reconcile(ctx, hash, deltas)
		if reconcileErr != nil {
			return fmt.Errorf("reconcile: %w", reconcileErr)
		}
		change.Deltas = append(change.Deltas, corrections...)
	}

	through := block.GetFinalizedHeight()
	if byDepth := height - indexer.options.HistoryDepth; byDepth > through {
		through = byDepth
	}
	indexer.prunedThrough = max(indexer.prunedThrough, through)
	indexer.pending = change
	return nil
}

// reconcile checks the least recently verified holdings that this block did
// not touch and returns the corrections that make them match balanceOf.
func (indexer *Indexer) reconcile(ctx context.Context, hash string, touched map[model.HolderKey]*big.Int) ([]model.HolderDelta, error) {
	stale, err := indexer.options.Store.StaleHoldings(ctx, indexer.options.ChainID, indexer.options.ReconcileBatch+len(touched))
	if err != nil {
		return nil, err
	}
	stale = slices.DeleteFunc(stale, func(holding model.Holding) bool {
		_, ok := touched[holding.HolderKey]
		return ok
	})
	stale = stale[:min(len(stale), indexer.options.ReconcileBatch)]
	keys := make([]model.HolderKey, len(stale))
	for index, holding := range stale {
		keys[index] = holding.HolderKey
	}
	actual, err := indexer.balances(ctx, hash, keys)
	if err != nil {
		return nil, err
	}
	corrections := make([]model.HolderDelta, len(stale))
	drifted := 0
	for index, holding := range stale {
		correction := model.HolderDelta{HolderKey: holding.HolderKey, Delta: new(big.Int), Verified: true}
		if actual[index] != nil {
			correction.Delta.Sub(actual[index], holding.Balance)
		}
		if correction.Delta.Sign() != 0 {
			drifted++
		}
		corrections[index] = correction
	}
	if drifted > 0 {
		indexer.logger.Info("reconciled holdings", "checked", len(stale), "corrected", drifted)
	}
	return corrections, nil
}

func (indexer *Indexer) balances(ctx context.Context, hash string, keys []model.HolderKey) ([]*big.Int, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	queries := make([]rpc.BalanceQuery, len(keys))
	for index, key := range keys {
		queries[index] = rpc.BalanceQuery{Token: key.Token, Holder: key.Holder}
	}
	return indexer.options.Balances.BalancesAt(ctx, hash, queries)
}

func (indexer *Indexer) Revert(_ context.Context, rollbackTo int64) error {
	if rollbackTo < indexer.prunedThrough {
		return fmt.Errorf("%w: rollback to %d, history starts after %d", ErrReorgTooDeep, rollbackTo, indexer.prunedThrough)
	}
	indexer.pending = model.HolderChange{Reverted: true, RevertTo: rollbackTo}
	return nil
}

func (indexer *Indexer) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	change := indexer.pending
	change.PrunedThrough = indexer.prunedThrough
	if err := indexer.options.Store.CommitHolders(ctx, checkpoint, change); err != nil {
		return err
	}
	indexer.pending = model.HolderChange{}
	return nil
}

// netTransfers sums each holder's net change per token. Mints and burns move
// balances to and from the zero address, which is not a holder.
func netTransfers(transfers []*streamv1.TokenTransfer) map[model.HolderKey]*big.Int {
	deltas := make(map[model.HolderKey]*big.Int)
	add := func(token, holder string, amount *big.Int) {
		if holder == zeroAddress || holder == "" {
			return
		}
		key := model.HolderKey{Token: token, Holder: holder}
		if deltas[key] == nil {
			deltas[key] = new(big.Int)
		}
		deltas[key].Add(deltas[key], amount)
	}
	for _, transfer := range transfers {
		amount, ok := new(big.Int).SetString(transfer.GetRawAmount(), 10)
		// Zero-value transfers change nothing; skipping them also keeps
		// spam that sends nothing to many addresses from seeding holdings.
		if !ok || amount.Sign() <= 0 {
			continue
		}
		token := normalize(transfer.GetToken())
		add(token, normalize(transfer.GetFrom()), new(big.Int).Neg(amount))
		add(token, normalize(transfer.GetTo()), amount)
	}
	return deltas
}

func normalize(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

func compareKeys(left, right model.HolderKey) int {
	return cmp.Or(cmp.Compare(left.Token, right.Token), cmp.Compare(left.Holder, right.Holder))
}

var _ chainsync.Handler = (*Indexer)(nil)
