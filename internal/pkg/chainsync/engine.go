// Package chainsync applies one chainstream-evm chain topic, in offset order,
// to a Handler: it checks continuity, handles reverts and redeliveries, and
// commits a checkpoint after every record.
//
// chainsync 包按 offset 顺序把一条 chainstream-evm 链 topic 应用到 Handler：
// 校验区块连续性、处理 revert 与重复投递，并在每条记录后提交检查点。
// market-engine、holder-indexer、market-registry、ch-loader 共用这一顺序应用循环。
package chainsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrChainMismatch means a record belongs to a different chain than the
	// engine was configured for, usually a misconfigured topic.
	ErrChainMismatch = errors.New("engine: record chain does not match")
	// ErrHeightGap means a block skipped heights without a revert.
	ErrHeightGap = errors.New("engine: block height gap")
	// ErrParentMismatch means a block does not extend the last applied block
	// and no revert preceded it.
	ErrParentMismatch = errors.New("engine: block parent does not match")
)

// SourceFactory opens the chain topic at a start offset.
type SourceFactory func(start int64) (source.Source, error)

// Options 配置一条链的消费引擎：链 ID、打开 topic 的工厂（kafka 或 file）、Handler。
type Options struct {
	ChainID uint64
	Chain   string
	Open    SourceFactory
	// Handler receives the records.
	Handler Handler
	Logger  *slog.Logger
	Now     func() time.Time
}

// Engine consumes one chain topic. Each record is applied and committed
// before the next, so a restart resumes at the first uncommitted record.
//
// Engine 消费一条链 topic，每条记录应用并提交后才处理下一条，所以重启后会从第一条
// 未提交的记录继续。
type Engine struct {
	chainID uint64
	chain   string
	open    SourceFactory
	handler Handler
	logger  *slog.Logger
	now     func() time.Time
}

func New(options Options) (*Engine, error) {
	if options.ChainID == 0 || options.Open == nil || options.Handler == nil {
		return nil, errors.New("engine: chain id, source and handler are required")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("chain", options.Chain, "chain_id", options.ChainID)
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{chainID: options.ChainID, chain: options.Chain, open: options.Open, handler: options.Handler, logger: logger, now: now}, nil
}

// Run consumes until ctx is done, returning nil, or a record fails to apply.
func (engine *Engine) Run(ctx context.Context) error {
	checkpoint, ok, err := engine.handler.Restore(ctx, engine.chainID)
	if err != nil {
		return fmt.Errorf("engine: restore: %w", err)
	}
	if !ok {
		checkpoint = Checkpoint{ChainID: engine.chainID, Offset: -1}
	}
	stream, err := engine.open(checkpoint.Offset + 1)
	if err != nil {
		return fmt.Errorf("engine: open source: %w", err)
	}
	defer stream.Close()
	engine.logger.Info("engine started", "offset", checkpoint.Offset+1, "height", checkpoint.Height)
	if applied(checkpoint) {
		metrics.Height.WithLabelValues(engine.chain).Set(float64(checkpoint.Height))
	}
	lagger, _ := stream.(source.Lagger)

	for {
		messages, fetchErr := stream.Fetch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetchErr != nil {
			return fmt.Errorf("engine: fetch: %w", fetchErr)
		}
		for _, message := range messages {
			checkpoint, err = engine.handle(ctx, checkpoint, message)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("engine: offset %d: %w", message.Offset, err)
			}
		}
		if lagger != nil {
			if end := lagger.HighWatermark(); end >= 0 {
				metrics.ConsumerLag.WithLabelValues(engine.chain).Set(float64(max(end-checkpoint.Offset-1, 0)))
			}
		}
	}
}

// handle 处理一条记录：offset 不大于检查点的直接跳过；revert 交给 revert，区块交给
// apply；最后调用 Handler.Commit 保存新的检查点并更新指标。
func (engine *Engine) handle(ctx context.Context, checkpoint Checkpoint, message source.Message) (Checkpoint, error) {
	if message.Offset <= checkpoint.Offset {
		return checkpoint, nil
	}
	var record streamv1.StreamRecord
	if err := proto.Unmarshal(message.Value, &record); err != nil {
		return checkpoint, fmt.Errorf("decode record: %w", err)
	}
	start := time.Now()
	next := checkpoint
	var kind string
	switch payload := record.GetPayload().(type) {
	case *streamv1.StreamRecord_Revert:
		kind = "revert"
		if err := engine.revert(ctx, checkpoint, payload.Revert, &next); err != nil {
			return checkpoint, err
		}
	case *streamv1.StreamRecord_Block:
		kind = "block"
		if err := engine.apply(ctx, checkpoint, payload.Block, &next); err != nil {
			return checkpoint, err
		}
	default:
		return checkpoint, fmt.Errorf("unknown record payload %T", payload)
	}
	next.Offset = message.Offset
	next.UpdatedAt = engine.now()
	if err := engine.handler.Commit(ctx, next); err != nil {
		return checkpoint, fmt.Errorf("commit: %w", err)
	}
	metrics.Records.WithLabelValues(engine.chain, kind).Inc()
	metrics.RecordSeconds.WithLabelValues(engine.chain, kind).Observe(time.Since(start).Seconds())
	if next.Height != checkpoint.Height {
		metrics.Height.WithLabelValues(engine.chain).Set(float64(next.Height))
	}
	if block := record.GetBlock(); block != nil && next.Height == block.GetBlockHeight() {
		lag := engine.now().Sub(time.Unix(block.GetBlockTime(), 0))
		metrics.BlockLag.WithLabelValues(engine.chain).Set(max(lag.Seconds(), 0))
	}
	return next, nil
}

// revert：revert 可能被重复投递，回滚高度不低于已应用高度时无事可做；否则调用
// Handler.Revert，并把检查点高度降到 rollback_to、清空哈希（下一个块不再校验 parent）。
func (engine *Engine) revert(ctx context.Context, checkpoint Checkpoint, revert *streamv1.BlockRevert, next *Checkpoint) error {
	if revert.GetChainId() != engine.chainID {
		return fmt.Errorf("%w: revert chain %d", ErrChainMismatch, revert.GetChainId())
	}
	rollbackTo := revert.GetRollbackTo()
	// Reverts may be redelivered; one at or above the applied height has
	// nothing left to undo.
	if !applied(checkpoint) || rollbackTo >= checkpoint.Height {
		return nil
	}
	if err := engine.handler.Revert(ctx, rollbackTo); err != nil {
		return fmt.Errorf("revert to %d: %w", rollbackTo, err)
	}
	engine.logger.Warn("reverted", "from", checkpoint.Height, "to", rollbackTo)
	metrics.Reorgs.WithLabelValues(engine.chain).Inc()
	metrics.ReorgDepth.WithLabelValues(engine.chain).Observe(float64(checkpoint.Height - rollbackTo))
	next.Height, next.Hash = rollbackTo, ""
	return nil
}

// apply 校验连续性：高度不大于已应用高度视为重复投递并跳过；跳高度或 parent hash
// 对不上（且之前没有 revert）说明数据异常，返回错误让引擎停止，而不是错误地继续。
func (engine *Engine) apply(ctx context.Context, checkpoint Checkpoint, block *streamv1.BlockEvent, next *Checkpoint) error {
	if block.GetChainId() != engine.chainID {
		return fmt.Errorf("%w: block chain %d", ErrChainMismatch, block.GetChainId())
	}
	height := block.GetBlockHeight()
	if applied(checkpoint) {
		// A retried publish can redeliver blocks that were already applied.
		if height <= checkpoint.Height {
			engine.logger.Debug("skipping redelivered block", "height", height, "applied", checkpoint.Height)
			return nil
		}
		if height != checkpoint.Height+1 {
			return fmt.Errorf("%w: got %d after %d", ErrHeightGap, height, checkpoint.Height)
		}
		if checkpoint.Hash != "" && block.GetParentHash() != checkpoint.Hash {
			return fmt.Errorf("%w: block %d parent %s, applied %s", ErrParentMismatch, height, block.GetParentHash(), checkpoint.Hash)
		}
	}
	if err := engine.handler.Apply(ctx, block); err != nil {
		return fmt.Errorf("apply block %d: %w", height, err)
	}
	next.Height, next.Hash = height, block.GetBlockHash()
	return nil
}

// applied reports whether the checkpoint has applied at least one block.
func applied(checkpoint Checkpoint) bool {
	return checkpoint.Height > 0 || checkpoint.Hash != ""
}
