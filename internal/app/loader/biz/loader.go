// Package biz is ch-loader's business layer: it copies a chain topic's raw facts into ClickHouse:
// transfers, unoriented trades, oracle updates, and asset and pool facts.
//
// ClickHouse has no transactions, so facts are written first and the
// checkpoint after them, in the same ClickHouse database. A crash between
// the two leaves facts above the checkpoint; the first block applied after a
// restart therefore deletes its height and everything above before
// inserting, which makes the replay idempotent.
//
// Blocks older than BatchLag are buffered and written together, up to
// FlushRows facts or FlushInterval, so catching up does not create a part per
// block. Newer blocks are written on their own commit.
//
// ch-loader 业务层：把链 topic 的原始事实（ERC-20 转账、未定方向的交易、预言机更新、
// 资产与池子事实）原样写入 ClickHouse。ClickHouse 没有事务，所以先写事实、后写检查点
// （同一个库）；崩溃重启后第一个区块会先删除其高度及以上的数据再插入，保证重放幂等。
// 追块时（区块早于 BatchLag）缓冲后批量写入，接近链头的区块随各自的提交立即写入。
package biz

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// RawRepo keeps raw facts by (chain, height). It is not transactional, so
// the loader stores facts first and the checkpoint after them; a checkpoint
// can lag the facts but never lead them.
type RawRepo interface {
	LoadRawCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error)
	// WriteRaw inserts blocks, which must not already be stored.
	WriteRaw(ctx context.Context, blocks []model.RawBlock) error
	// DeleteRawAbove removes every fact above height.
	DeleteRawAbove(ctx context.Context, chainID uint64, height int64) error
	SaveRawCheckpoint(ctx context.Context, checkpoint chainsync.Checkpoint) error
}

type Options struct {
	ChainID uint64
	Repo    RawRepo
	// BatchLag is the block age above which blocks are buffered. Zero
	// writes every block on its own commit.
	BatchLag time.Duration
	// FlushRows and FlushInterval bound a buffered batch.
	FlushRows     int
	FlushInterval time.Duration
	Logger        *slog.Logger
	Now           func() time.Time
}

// Loader is an chainsync.Handler for one chain. It is not safe for concurrent
// use.
type Loader struct {
	options Options
	logger  *slog.Logger
	now     func() time.Time

	pending    []model.RawBlock
	rows       int
	checkpoint chainsync.Checkpoint
	dirty      bool
	lastFlush  time.Time
	// recovering is true until the first block or revert after Restore has
	// cleared whatever an interrupted write left above the checkpoint.
	recovering bool
	// stored is the highest height that may have stored facts.
	stored int64
	// unsure is set while a flush has not completed.
	unsure bool
}

func New(options Options) (*Loader, error) {
	if options.ChainID == 0 || options.Repo == nil {
		return nil, errors.New("chloader: chain id and repo are required")
	}
	if options.BatchLag < 0 || options.FlushRows < 0 || options.FlushInterval < 0 {
		return nil, errors.New("chloader: batch settings must not be negative")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Loader{options: options, logger: logger, now: now, recovering: true, lastFlush: now()}, nil
}

func (loader *Loader) Restore(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	checkpoint, ok, err := loader.options.Repo.LoadRawCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	loader.stored = checkpoint.Height
	return checkpoint, true, nil
}

func (loader *Loader) Apply(ctx context.Context, block *streamv1.BlockEvent) error {
	raw, err := model.NewRawBlock(block)
	if err != nil {
		return err
	}
	if loader.recovering {
		if err := loader.options.Repo.DeleteRawAbove(ctx, loader.options.ChainID, raw.Height-1); err != nil {
			return err
		}
		loader.recovering, loader.stored = false, raw.Height-1
	}
	loader.pending = append(loader.pending, raw)
	loader.rows += raw.Rows()
	return nil
}

// Revert drops buffered blocks above rollbackTo and deletes stored ones.
func (loader *Loader) Revert(ctx context.Context, rollbackTo int64) error {
	loader.pending = slices.DeleteFunc(loader.pending, func(block model.RawBlock) bool {
		return block.Height > rollbackTo
	})
	loader.rows = 0
	for _, block := range loader.pending {
		loader.rows += block.Rows()
	}
	if loader.recovering || loader.stored > rollbackTo {
		if err := loader.options.Repo.DeleteRawAbove(ctx, loader.options.ChainID, rollbackTo); err != nil {
			return err
		}
		loader.recovering = false
		loader.stored = rollbackTo
	}
	return nil
}

// Commit writes the buffered facts and then checkpoint, unless the batch
// may still grow.
func (loader *Loader) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	loader.checkpoint, loader.dirty = checkpoint, true
	if !loader.due() {
		return nil
	}
	return loader.Flush(ctx)
}

func (loader *Loader) due() bool {
	if len(loader.pending) == 0 {
		// A revert or a skipped record only moves the checkpoint.
		return loader.options.FlushInterval == 0 || loader.now().Sub(loader.lastFlush) >= loader.options.FlushInterval
	}
	last := loader.pending[len(loader.pending)-1]
	switch {
	case loader.now().Sub(last.BlockTime) < loader.options.BatchLag || loader.options.BatchLag == 0:
		return true
	case loader.options.FlushRows > 0 && loader.rows >= loader.options.FlushRows:
		return true
	default:
		return loader.now().Sub(loader.lastFlush) >= loader.options.FlushInterval
	}
}

// Flush writes everything buffered and the latest checkpoint.
func (loader *Loader) Flush(ctx context.Context) error {
	if !loader.dirty {
		return nil
	}
	// A failed flush may have stored part of the batch; clear it first.
	if loader.unsure && len(loader.pending) > 0 {
		if err := loader.options.Repo.DeleteRawAbove(ctx, loader.options.ChainID, loader.pending[0].Height-1); err != nil {
			return err
		}
	}
	loader.unsure = true
	if err := loader.options.Repo.WriteRaw(ctx, loader.pending); err != nil {
		return err
	}
	if len(loader.pending) > 0 {
		loader.stored = max(loader.stored, loader.pending[len(loader.pending)-1].Height)
	}
	if err := loader.options.Repo.SaveRawCheckpoint(ctx, loader.checkpoint); err != nil {
		return err
	}
	if len(loader.pending) > 1 {
		loader.logger.Debug("flushed raw facts", "blocks", len(loader.pending), "rows", loader.rows, "height", loader.checkpoint.Height)
	}
	loader.pending, loader.rows, loader.dirty, loader.unsure = loader.pending[:0], 0, false, false
	loader.lastFlush = loader.now()
	return nil
}

var _ chainsync.Handler = (*Loader)(nil)
