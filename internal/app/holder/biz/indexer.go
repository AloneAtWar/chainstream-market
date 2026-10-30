// Package biz is holder-indexer's business layer: it tracks ERC-20 balances per holder from a chain topic's
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
// Native coin balances (ETH, BNB) are kept the same way under the token
// rpc.NativeToken when NativeBalances is set. Gas, rewards and L2 fees move
// them without a transfer, so instead of summing transfers every address in
// the block's native_touched is read again at the block's hash, and the
// difference from the stored balance is applied as a verified delta.
//
// Balances live only in the store, never in memory, and every change is
// stored as an undoable delta, so a reorg subtracts what the reverted blocks
// added.
//
// holder-indexer 业务层：根据链 topic 维护每个持有人的 ERC-20（及可选原生币）余额。
//   - 播种：(代币, 持有人) 首次出现时，在该区块哈希处读取 balanceOf，已包含本块转账；
//   - 累加：之后每个区块只叠加净转账；
//   - 对账：每 ReconcileEvery 个块，挑最久未校验的持仓重新读 balanceOf 修正
//     （处理 rebase、转账收费等不产生 Transfer 的余额变化）；
//   - 原生币：不累加转账，而是对 native_touched 中的地址在区块哈希处重读余额，
//     差值作为已校验的 delta；
//   - 重组：所有变化都以可撤销 delta 存储，回滚时减去被回滚区块加上的部分。
//
// 余额只存在于 PostgreSQL，内存不随持有人数量增长。
package biz

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
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
var ErrReorgTooDeep = errors.New("holders: reorg deeper than retained history")

const zeroAddress = "0x0000000000000000000000000000000000000000"

// Balances reads balanceOf at a block; *rpc.Client implements it.
type Balances interface {
	BalancesAt(ctx context.Context, blockHash string, queries []rpc.BalanceQuery) ([]*big.Int, error)
}

// HolderRepo persists holder balances. CommitHolders applies a change and
// saves the checkpoint atomically.
type HolderRepo interface {
	// LoadHolderCheckpoint returns the last committed checkpoint and the
	// highest height whose undo history was pruned.
	LoadHolderCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error)
	// Holdings returns the stored holdings among keys.
	Holdings(ctx context.Context, chainID uint64, keys []model.HolderKey) ([]model.Holding, error)
	// StaleHoldings returns up to limit holdings, least recently verified
	// first, leaving out those of skipToken unless it is empty.
	StaleHoldings(ctx context.Context, chainID uint64, limit int, skipToken string) ([]model.Holding, error)
	CommitHolders(ctx context.Context, checkpoint chainsync.Checkpoint, change model.HolderChange) error
}

type Options struct {
	ChainID  uint64
	Repo     HolderRepo
	Balances Balances
	// HistoryDepth is how many blocks of undo history to keep in addition
	// to everything above the finalized height.
	HistoryDepth int64
	// ReconcileEvery is how often, in blocks, stale holdings are checked
	// against balanceOf. Zero disables reconciliation.
	ReconcileEvery int64
	// ReconcileBatch is how many holdings each reconciliation checks.
	ReconcileBatch int
	// NativeBalances keeps native coin balances of the addresses each
	// block's native_touched lists. Balances must then answer queries for
	// rpc.NativeToken.
	NativeBalances bool
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
	if options.ChainID == 0 || options.Repo == nil || options.Balances == nil {
		return nil, errors.New("holders: chain id, repo and balances are required")
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
	checkpoint, prunedThrough, ok, err := indexer.options.Repo.LoadHolderCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	indexer.prunedThrough = prunedThrough
	return checkpoint, true, nil
}

// Apply 汇总本块净转账，为新持仓播种、按周期对账、刷新原生币余额，生成待提交的 delta。
func (indexer *Indexer) Apply(ctx context.Context, block *streamv1.BlockEvent) error {
	height, hash := block.GetBlockHeight(), block.GetBlockHash()
	deltas := netTransfers(block.GetTransfers())
	var native []model.HolderKey
	if indexer.options.NativeBalances {
		native = nativeTouched(block.GetNativeTouched())
	}
	keys := make([]model.HolderKey, 0, len(deltas)+len(native))
	for key := range deltas {
		keys = append(keys, key)
	}
	keys = append(keys, native...)
	slices.SortFunc(keys, compareKeys)

	known, err := indexer.options.Repo.Holdings(ctx, indexer.options.ChainID, keys)
	if err != nil {
		return fmt.Errorf("load holdings: %w", err)
	}
	stored := make(map[model.HolderKey]*big.Int, len(known))
	for _, holding := range known {
		stored[holding.HolderKey] = holding.Balance
	}
	change := model.HolderChange{Height: height}
	// read holds every key whose balance is read at this block: new
	// holdings, and every native holding the block touched.
	var read []model.HolderKey
	for _, key := range keys {
		if _, ok := stored[key]; ok && key.Token != rpc.NativeToken {
			if deltas[key].Sign() != 0 {
				change.Deltas = append(change.Deltas, model.HolderDelta{HolderKey: key, Delta: deltas[key]})
			}
			continue
		}
		read = append(read, key)
	}
	balances, err := indexer.balances(ctx, hash, read)
	if err != nil {
		return fmt.Errorf("seed holdings: %w", err)
	}
	for index, key := range read {
		balance, existing := balances[index], stored[key]
		switch {
		case key.Token == rpc.NativeToken && existing != nil:
			if balance != nil && balance.Cmp(existing) != 0 {
				change.Deltas = append(change.Deltas, model.HolderDelta{
					HolderKey: key, Delta: new(big.Int).Sub(balance, existing), Verified: true,
				})
			}
		case key.Token == rpc.NativeToken:
			// Senders and fee recipients are touched by every block; only
			// addresses that hold something are kept.
			if balance != nil && balance.Sign() > 0 {
				change.Created = append(change.Created, model.HolderSeed{Holding: model.Holding{HolderKey: key, Balance: balance}, Verified: true})
			}
		default:
			seed := model.HolderSeed{Holding: model.Holding{HolderKey: key, Balance: balance}, Verified: true}
			if seed.Balance == nil {
				// balanceOf failed; the block's net transfers are the best guess.
				seed.Balance, seed.Verified = deltas[key], false
			}
			change.Created = append(change.Created, seed)
		}
	}

	if every := indexer.options.ReconcileEvery; every > 0 && height%every == 0 {
		touched := make(map[model.HolderKey]struct{}, len(keys))
		for _, key := range keys {
			touched[key] = struct{}{}
		}
		corrections, reconcileErr := indexer.reconcile(ctx, hash, touched)
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
func (indexer *Indexer) reconcile(ctx context.Context, hash string, touched map[model.HolderKey]struct{}) ([]model.HolderDelta, error) {
	// Native holdings are only reconciled while native balances are on.
	// They are left out by the query, since holdings kept from before
	// native balances were turned off are never verified again and would
	// otherwise fill every batch.
	skip := ""
	if !indexer.options.NativeBalances {
		skip = rpc.NativeToken
	}
	stale, err := indexer.options.Repo.StaleHoldings(ctx, indexer.options.ChainID, indexer.options.ReconcileBatch+len(touched), skip)
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
	if err := indexer.options.Repo.CommitHolders(ctx, checkpoint, change); err != nil {
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

// nativeTouched returns the native holding keys of a block's touched
// addresses, without the zero address and duplicates.
func nativeTouched(addresses []string) []model.HolderKey {
	keys := make([]model.HolderKey, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		holder := normalize(address)
		if _, ok := seen[holder]; ok || holder == "" || holder == zeroAddress {
			continue
		}
		seen[holder] = struct{}{}
		keys = append(keys, model.HolderKey{Token: rpc.NativeToken, Holder: holder})
	}
	return keys
}

func normalize(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

func compareKeys(left, right model.HolderKey) int {
	return cmp.Or(cmp.Compare(left.Token, right.Token), cmp.Compare(left.Holder, right.Holder))
}

var _ chainsync.Handler = (*Indexer)(nil)
