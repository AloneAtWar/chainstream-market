package biz

import (
	"context"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

// The repositories market-engine persists through, which the data layer
// implements. Every write is keyed by (chain, block height) so that
// replaying a block after a crash, or rewinding after a reorg, leaves
// exactly one copy of each fact. The handler saves its checkpoint only after
// Output.WriteBlock returns, so a store must tolerate the same block being
// written twice.

// CheckpointStore persists one chainsync.Checkpoint per chain.
//
// CheckpointStore 为每条链保存一个消费检查点（offset、高度、区块哈希）。
type CheckpointStore interface {
	Load(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint chainsync.Checkpoint) error
}

// Output stores what the processor derives from each block.
//
// Output 保存每个区块的派生结果（交易、价格、K 线）。WriteBlock 必须覆盖同一高度
// 已有的数据（崩溃后会重放一个块），Rewind 删除 rollbackTo 之上的数据并恢复 K 线等
// 累计值。
type Output interface {
	// WriteBlock replaces everything previously written for block.Height.
	WriteBlock(ctx context.Context, block model.BlockOutput) error
	// Rewind removes everything written above rollbackTo and restores
	// running aggregates, such as open candles, to their state at that height.
	Rewind(ctx context.Context, chainID uint64, rollbackTo int64) error
}

// History reads what earlier blocks stored: ch-loader's raw trades and
// market-engine's own token prices. The processor uses it to recompute the
// trades of a token whose decimals arrived late, beyond the ones it held.
//
// History 读取更早区块已存储的数据（ch-loader 的原始交易、engine 自己的历史价格），
// 用于在代币小数位迟到时回填并重算其交易。
type History interface {
	// RawTrades returns, in chain order, the trades at heights from..through
	// with a leg in one of tokens. ok is false when the raw store does not
	// hold every height through yet.
	RawTrades(ctx context.Context, chainID uint64, tokens []string, from, through int64) (trades []model.RawTradeAt, ok bool, err error)
	// PriceHistory returns each token's stored prices at heights
	// from..through, oldest first, led by its latest price below from.
	PriceHistory(ctx context.Context, chainID uint64, tokens []string, from, through int64) (map[string][]model.PricePoint, error)
}

// State is a processor's persisted state and the checkpoint it belongs to.
// The engine restores it into a fresh processor on start, so a restart
// resumes at Checkpoint.Offset+1 without replaying the topic.
//
// State 是持久化的 Processor 状态及其所属检查点，重启时恢复后从 Offset+1 继续，无需重放 topic。
type State struct {
	Checkpoint chainsync.Checkpoint
	// PrunedThrough is the highest height whose undo history was discarded.
	PrunedThrough int64
	Entries       []StateEntry
	// Undo is the retained reorg history in ascending height order.
	Undo []StateUndo
}

// StateEntry is one key of processor state. Kind names the map the key
// belongs to, such as pools or prices. Value is opaque to storage; a nil
// Value in a StateDelta deletes the key.
type StateEntry struct {
	Kind  string
	Key   string
	Value []byte
}

// StateUndo is the value a key held before the first write at Height. A nil
// Prev means the key did not exist.
type StateUndo struct {
	Kind   string
	Height int64
	Key    string
	Prev   []byte
}

// StateDelta is what one record changed in a processor's state.
//
// StateDelta 是处理一条记录后状态的变化量（新值、撤销记录、回滚标记、修剪高度）。
type StateDelta struct {
	// Reverted reports a reorg: undo history above RevertTo is discarded
	// before Undo is added.
	Reverted bool
	RevertTo int64
	Entries  []StateEntry
	Undo     []StateUndo
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}

// StateStore persists processor state. Commit applies a delta and saves the
// checkpoint atomically, so state never runs ahead of or behind the record
// it was derived from.
//
// StateStore 持久化 Processor 状态。Commit 必须在一个事务里应用增量并保存检查点，
// 保证状态与记录永远对齐（PostgreSQL 实现见 data 层）。
type StateStore interface {
	LoadState(ctx context.Context, chainID uint64) (State, bool, error)
	Commit(ctx context.Context, checkpoint chainsync.Checkpoint, delta StateDelta) error
}
