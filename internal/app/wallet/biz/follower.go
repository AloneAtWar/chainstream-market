// Package biz 是 market-wallet 的业务层：按移动加权平均成本维护每个钱包每个代币的持仓
// 账本，定时汇总 7d/30d/90d 的钱包统计，并按规则给钱包打聪明钱、机器人等标签。
//
// 输入是 market-engine 的 live topic：每个区块已定价、已定方向的成交，以及重组时的回滚
// 高度。Follower 按 offset 顺序读它，把成交记入账本，回滚时用 undo 表恢复持仓。live
// topic 可能缺块：market-engine 追块时不推送比 live.max_lag 更旧的区块。缺的高度从
// ClickHouse 的 trades 表补：market-engine 先写存储再推送，所以推送某个区块时，它之前
// 的区块都已经在 ClickHouse 里。首次启动从 live topic 末尾开始读，把 wallet.history
// 之内的历史当作缺块补上。
//
// 持仓存 PostgreSQL，逐笔流水存 ClickHouse（wallet_trades）。ClickHouse 没有事务，所以
// 每次提交先写流水，再在一个 PostgreSQL 事务里写持仓、undo 和检查点；崩溃后启动时先删掉
// 检查点之后写入的流水，再重放。
package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	"github.com/shopspring/decimal"
)

// WholeBlock 是检查点的 Part 取值，表示该高度的区块已经全部应用（回放的高度、回滚到的高度）。
const WholeBlock int32 = -1

// Checkpoint 是最后一条完整应用并提交的 live 记录。
type Checkpoint struct {
	ChainID uint64
	// Offset 是 live topic 里最后一条已应用记录的 offset；从 Offset+1 继续读。-1 表示还
	// 没有读到过记录（首次启动的回放中）。
	Offset int64
	// Height 和 Part 是最后应用的区块和它的分片；Part 为 WholeBlock 时整个区块都已应用。
	Height int64
	Part   int32
	// PrunedThrough 是 undo 已经裁剪到的高度。
	PrunedThrough int64
	UpdatedAt     time.Time
}

// Undo 是一个持仓在某个高度第一次被改动之前的样子；Prev 为 nil 表示该高度新建了这个持仓。
type Undo struct {
	Height int64
	Key    model.WalletKey
	Prev   *model.Position
}

// Commit 是一次提交：先写 Trades，再在一个事务里写 Positions、Undo 并保存 Checkpoint。
type Commit struct {
	Checkpoint Checkpoint
	Positions  []model.Position
	// Undo 只包含高度大于 Checkpoint.PrunedThrough 的条目。
	Undo   []Undo
	Trades []model.WalletTrade
}

// WalletRepo 保存账本。持仓、undo 和检查点在 PostgreSQL 的同一个事务里；流水在 ClickHouse，
// 先于事务写入。
type WalletRepo interface {
	LoadWalletCheckpoint(ctx context.Context, chainID uint64) (Checkpoint, bool, error)
	// DiscardTradesAfter 删除 ClickHouse 里检查点之后写入的流水：apply_height 大于 height，
	// 或等于 height 且 part 大于 part 的行（part 为 WholeBlock 时只删更高的高度）。
	DiscardTradesAfter(ctx context.Context, chainID uint64, height int64, part int32) error
	// Positions 返回 keys 中已存在的持仓。
	Positions(ctx context.Context, chainID uint64, keys []model.WalletKey) ([]model.Position, error)
	CommitWallet(ctx context.Context, commit Commit) error
	// RevertWallet 先删除 apply_height 大于 to 的流水，再在一个事务里用 undo 恢复持仓、
	// 删除对应 undo 并保存 checkpoint。
	RevertWallet(ctx context.Context, to int64, checkpoint Checkpoint) error
}

// TradeSource 读 market-engine 写在 ClickHouse 里的成交，用来补 live topic 缺的高度。
type TradeSource interface {
	// FirstHeightSince 返回 block_time 不早于 since 的最低已存区块高度。
	FirstHeightSince(ctx context.Context, chainID uint64, since time.Time) (int64, bool, error)
	// TradesBetween 返回 revision_height 在 [from, to] 之间的买卖成交，按 revision_height、
	// block_height、log_index、operation_index 升序。
	TradesBetween(ctx context.Context, chainID uint64, from, to int64) ([]Trade, error)
}

// Opener 打开链的 live topic：start 小于 0 时从末尾开始读，否则从 offset start 开始。
type Opener func(start int64) (source.Source, error)

// Options 配置一条链的 Follower。
type Options struct {
	ChainID uint64
	Chain   string
	Open    Opener
	Repo    WalletRepo
	Trades  TradeSource
	// HistoryDepth 是除了终局高度以上的区块之外，还保留可回滚的最近区块数。
	HistoryDepth int64
	// History 是首次启动从 ClickHouse 回放的时长。
	History time.Duration
	// FinalAge：从 ClickHouse 补的区块比它更旧时不留 undo。
	FinalAge time.Duration
	// BackfillHeights 是补缺时一次读多少个高度。
	BackfillHeights int64
	// BatchLag、FlushRows、FlushInterval 控制追块时的批量提交：区块比 BatchLag 新时每条
	// 记录都提交，否则攒到 FlushRows 条流水或 FlushInterval 再提交。
	BatchLag      time.Duration
	FlushRows     int
	FlushInterval time.Duration
	// CloseRatio：卖出后剩余不到这个比例的持仓视为清仓。
	CloseRatio float64
	Logger     *slog.Logger
	Now        func() time.Time
}

// Follower 跟读一条链的 live topic 并维护该链的持仓账本。不能并发使用。
type Follower struct {
	options    Options
	closeRatio decimal.Decimal
	logger     *slog.Logger
	now        func() time.Time

	checkpoint Checkpoint
	// started 表示已经有过检查点（不是首次启动）。
	started   bool
	finalized int64

	// 未提交的变更。positions 是被改动的持仓的当前状态，undo 记录每个（高度, 持仓）第一次
	// 改动之前的状态。
	positions map[model.WalletKey]*model.Position
	undo      []Undo
	undone    map[undoKey]struct{}
	trades    []model.WalletTrade
	dirty     bool
	lastFlush time.Time
	// pruneAge 是补缺时比 FinalAge 更旧的最高高度，这些高度不留 undo。
	pruneAge int64
	// blockTime 是最后应用的区块时间，用于 metrics。
	blockTime time.Time
}

type undoKey struct {
	height int64
	key    model.WalletKey
}

// New 创建 Follower。
func New(options Options) (*Follower, error) {
	if options.ChainID == 0 || options.Open == nil || options.Repo == nil || options.Trades == nil {
		return nil, errors.New("wallet: chain id, opener, repo and trade source are required")
	}
	if options.HistoryDepth < 0 || options.BackfillHeights <= 0 || options.FlushRows < 0 ||
		options.BatchLag < 0 || options.FlushInterval < 0 || options.History < 0 || options.FinalAge < 0 {
		return nil, errors.New("wallet: invalid ledger settings")
	}
	if options.CloseRatio < 0 || options.CloseRatio >= 1 {
		return nil, errors.New("wallet: close ratio must be in [0, 1)")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Follower{
		options: options, closeRatio: decimal.NewFromFloat(options.CloseRatio), logger: logger, now: now,
		positions: make(map[model.WalletKey]*model.Position), undone: make(map[undoKey]struct{}),
	}, nil
}

// Run 跟读 live topic 直到 ctx 结束（返回 nil）或出错。
func (follower *Follower) Run(ctx context.Context) error {
	if err := follower.restore(ctx); err != nil {
		return err
	}
	start := int64(-1)
	if follower.started && follower.checkpoint.Offset >= 0 {
		start = follower.checkpoint.Offset + 1
	}
	stream, err := follower.options.Open(start)
	if err != nil {
		return fmt.Errorf("wallet: open live topic: %w", err)
	}
	defer stream.Close()
	follower.logger.Info("wallet ledger started", "offset", start, "height", follower.checkpoint.Height)
	for {
		messages, fetchErr := stream.Fetch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetchErr != nil {
			return fmt.Errorf("wallet: fetch: %w", fetchErr)
		}
		for _, message := range messages {
			if err := follower.Handle(ctx, message); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("wallet: offset %d: %w", message.Offset, err)
			}
		}
	}
}

// restore 读检查点，并删掉崩溃前写进 ClickHouse 但没有提交的流水。
func (follower *Follower) restore(ctx context.Context) error {
	options := follower.options
	checkpoint, ok, err := options.Repo.LoadWalletCheckpoint(ctx, options.ChainID)
	if err != nil {
		return fmt.Errorf("wallet: restore: %w", err)
	}
	if !ok {
		checkpoint = Checkpoint{ChainID: options.ChainID, Offset: -1, Part: WholeBlock}
	}
	if err := options.Repo.DiscardTradesAfter(ctx, options.ChainID, checkpoint.Height, checkpoint.Part); err != nil {
		return fmt.Errorf("wallet: discard uncommitted trades: %w", err)
	}
	follower.checkpoint, follower.started = checkpoint, ok
	follower.lastFlush = follower.now()
	return nil
}

// Handle 处理 live topic 的一条记录。Run 之外只在测试里直接调用，调用前需要先 restore。
func (follower *Follower) Handle(ctx context.Context, message source.Message) error {
	if message.Offset <= follower.checkpoint.Offset {
		return nil
	}
	var update live.Update
	if err := json.Unmarshal(message.Value, &update); err != nil {
		return fmt.Errorf("decode live record: %w", err)
	}
	if update.ChainID != follower.options.ChainID {
		return fmt.Errorf("live record is for chain %d", update.ChainID)
	}
	switch update.Type {
	case live.UpdateRevert:
		return follower.revert(ctx, message.Offset, update)
	case live.UpdateBlock:
		return follower.block(ctx, message.Offset, update)
	default:
		return fmt.Errorf("unknown live record type %q", update.Type)
	}
}

// block 检查连续性并应用一个区块分片：重复投递的跳过；高度跳过的部分先从 ClickHouse 补；
// 同一区块缺了分片则报错停止，而不是带着缺失继续。
func (follower *Follower) block(ctx context.Context, offset int64, update live.Update) error {
	applied := follower.checkpoint
	height, part := update.Height, int32(update.Part)
	if update.FinalizedHeight > follower.finalized {
		follower.finalized = update.FinalizedHeight
	}
	switch {
	case !follower.started && applied.Height == 0:
		// 首次启动：从 live topic 末尾读到的第一个完整区块之前，按 wallet.history 回放。
		if part != 0 {
			return nil
		}
		if err := follower.bootstrap(ctx, offset, height); err != nil {
			return err
		}
	case height < applied.Height || (height == applied.Height && (applied.Part == WholeBlock || part <= applied.Part)):
		// market-engine 重启后会重发已经推送过的区块。
		return nil
	case height == applied.Height && part == applied.Part+1:
	case height > applied.Height && part == 0:
		if height > applied.Height+1 {
			if err := follower.backfill(ctx, offset, applied.Height+1, height-1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("live record height %d part %d does not follow height %d part %d", height, part, applied.Height, applied.Part)
	}
	trades := make([]Trade, 0, len(update.Trades))
	for _, liveTrade := range update.Trades {
		trade, err := fromLive(liveTrade, height)
		if err != nil {
			return err
		}
		trades = append(trades, trade)
	}
	if err := follower.apply(ctx, height, part, trades); err != nil {
		return err
	}
	follower.checkpoint.Offset, follower.checkpoint.Height, follower.checkpoint.Part = offset, height, part
	follower.dirty = true
	blockTime := time.Unix(update.BlockTime, 0)
	follower.blockTime = blockTime
	if follower.due(blockTime) {
		return follower.flush(ctx)
	}
	return nil
}

// bootstrap 在首次启动时回放 wallet.history 之内、第一个 live 区块之前的成交。
func (follower *Follower) bootstrap(ctx context.Context, offset, height int64) error {
	options := follower.options
	follower.started = true
	from, ok, err := options.Trades.FirstHeightSince(ctx, options.ChainID, follower.now().Add(-options.History))
	if err != nil {
		return fmt.Errorf("find replay start: %w", err)
	}
	if !ok || from >= height {
		follower.checkpoint.Height, follower.checkpoint.Part = height-1, WholeBlock
		return nil
	}
	follower.logger.Info("replaying wallet history", "from", from, "to", height-1)
	follower.checkpoint.Height, follower.checkpoint.Part = from-1, WholeBlock
	return follower.backfill(ctx, offset, from, height-1)
}

// backfill 从 ClickHouse 补 [from, to] 的成交，每 BackfillHeights 个高度提交一次。检查点的
// offset 停在当前记录之前，所以中途重启会从这条记录继续补。
func (follower *Follower) backfill(ctx context.Context, offset, from, to int64) error {
	options := follower.options
	for start := from; start <= to; start += options.BackfillHeights {
		end := min(start+options.BackfillHeights-1, to)
		trades, err := options.Trades.TradesBetween(ctx, options.ChainID, start, end)
		if err != nil {
			return fmt.Errorf("read trades %d-%d: %w", start, end, err)
		}
		for index := 0; index < len(trades); {
			next := index + 1
			for next < len(trades) && trades[next].RevisionHeight == trades[index].RevisionHeight {
				next++
			}
			height := trades[index].RevisionHeight
			if err := follower.apply(ctx, height, WholeBlock, trades[index:next]); err != nil {
				return err
			}
			follower.blockTime = trades[next-1].BlockTime
			if trades[next-1].BlockTime.Before(follower.now().Add(-options.FinalAge)) {
				follower.pruneAge = max(follower.pruneAge, height)
			}
			index = next
		}
		follower.checkpoint.Offset = max(follower.checkpoint.Offset, offset-1)
		follower.checkpoint.Height, follower.checkpoint.Part = end, WholeBlock
		follower.dirty = true
		if err := follower.flush(ctx); err != nil {
			return err
		}
	}
	metrics.WalletBackfills.WithLabelValues(options.Chain).Inc()
	follower.logger.Info("filled wallet gap from clickhouse", "from", from, "to", to)
	return nil
}

// apply 把一个区块（或分片）的成交记入账本，变更留在内存里等下次提交。
func (follower *Follower) apply(ctx context.Context, height int64, part int32, trades []Trade) error {
	var missing []model.WalletKey
	seen := make(map[model.WalletKey]struct{})
	for _, trade := range trades {
		if !tracked(trade) {
			continue
		}
		key := model.WalletKey{Wallet: trade.Trader, Token: trade.BaseToken}
		if _, ok := follower.positions[key]; ok {
			continue
		}
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		stored, err := follower.options.Repo.Positions(ctx, follower.options.ChainID, missing)
		if err != nil {
			return err
		}
		for index := range stored {
			position := stored[index]
			follower.positions[position.Key()] = &position
		}
		for _, key := range missing {
			if _, ok := follower.positions[key]; !ok {
				// nil 表示这个持仓还不存在。
				follower.positions[key] = nil
			}
		}
	}
	for _, trade := range trades {
		if !tracked(trade) {
			continue
		}
		key := model.WalletKey{Wallet: trade.Trader, Token: trade.BaseToken}
		current := follower.positions[key]
		marker := undoKey{height: height, key: key}
		if _, ok := follower.undone[marker]; !ok {
			follower.undone[marker] = struct{}{}
			var prev *model.Position
			if current != nil {
				copied := *current
				prev = &copied
			}
			follower.undo = append(follower.undo, Undo{Height: height, Key: key, Prev: prev})
		}
		if current == nil {
			current = &model.Position{}
			follower.positions[key] = current
		}
		entry := record(current, trade, follower.closeRatio)
		current.UpdatedHeight = height
		entry.ChainID, entry.ApplyHeight, entry.Part = follower.options.ChainID, height, part
		follower.trades = append(follower.trades, entry)
	}
	return nil
}

// due 判断是否该提交：接近链头的区块每条都提交，追块时攒批。
func (follower *Follower) due(blockTime time.Time) bool {
	options := follower.options
	now := follower.now()
	switch {
	case options.BatchLag == 0 || now.Sub(blockTime) < options.BatchLag:
		return true
	case options.FlushRows > 0 && len(follower.trades) >= options.FlushRows:
		return true
	default:
		return now.Sub(follower.lastFlush) >= options.FlushInterval
	}
}

// flush 提交内存里的变更和检查点。
func (follower *Follower) flush(ctx context.Context) error {
	if !follower.dirty {
		return nil
	}
	checkpoint := follower.checkpoint
	checkpoint.ChainID = follower.options.ChainID
	checkpoint.UpdatedAt = follower.now().UTC()
	checkpoint.PrunedThrough = max(checkpoint.PrunedThrough, follower.prunable(checkpoint.Height))
	commit := Commit{Checkpoint: checkpoint, Trades: follower.trades}
	for _, key := range slices.SortedFunc(maps.Keys(follower.positions), compareKeys) {
		if position := follower.positions[key]; position != nil {
			commit.Positions = append(commit.Positions, *position)
		}
	}
	for _, entry := range follower.undo {
		if entry.Height > checkpoint.PrunedThrough {
			commit.Undo = append(commit.Undo, entry)
		}
	}
	if err := follower.options.Repo.CommitWallet(ctx, commit); err != nil {
		return err
	}
	chain := follower.options.Chain
	metrics.WalletHeight.WithLabelValues(chain).Set(float64(checkpoint.Height))
	metrics.WalletTrades.WithLabelValues(chain).Add(float64(len(commit.Trades)))
	if !follower.blockTime.IsZero() {
		metrics.WalletBlockLag.WithLabelValues(chain).Set(follower.now().Sub(follower.blockTime).Seconds())
	}
	follower.checkpoint = checkpoint
	follower.reset()
	return nil
}

// prunable 返回可以裁剪 undo 的高度：终局高度和 height − HistoryDepth 中较低的那个，以及
// 补缺时已经足够旧的高度。
func (follower *Follower) prunable(height int64) int64 {
	through := height - follower.options.HistoryDepth
	if follower.finalized > 0 {
		through = min(through, follower.finalized)
	} else {
		through = 0
	}
	return max(through, min(follower.pruneAge, height))
}

func (follower *Follower) reset() {
	clear(follower.positions)
	clear(follower.undone)
	follower.undo, follower.trades = nil, nil
	follower.dirty = false
	follower.lastFlush = follower.now()
}

// revert 先提交内存里的变更，再把账本回滚到 rollback_to。重复投递的回滚（不低于已应用
// 高度）什么都不做。
func (follower *Follower) revert(ctx context.Context, offset int64, update live.Update) error {
	to := update.Height
	if update.Part != 0 || to >= follower.checkpoint.Height || !follower.started {
		return nil
	}
	if err := follower.flush(ctx); err != nil {
		return err
	}
	checkpoint := follower.checkpoint
	checkpoint.Offset, checkpoint.Height, checkpoint.Part = offset, to, WholeBlock
	checkpoint.UpdatedAt = follower.now().UTC()
	if err := follower.options.Repo.RevertWallet(ctx, to, checkpoint); err != nil {
		return fmt.Errorf("revert to %d: %w", to, err)
	}
	follower.logger.Warn("wallet ledger reverted", "from", follower.checkpoint.Height, "to", to)
	follower.checkpoint = checkpoint
	follower.pruneAge = min(follower.pruneAge, to)
	return nil
}

func compareKeys(left, right model.WalletKey) int {
	if left.Wallet != right.Wallet {
		if left.Wallet < right.Wallet {
			return -1
		}
		return 1
	}
	switch {
	case left.Token < right.Token:
		return -1
	case left.Token > right.Token:
		return 1
	default:
		return 0
	}
}

// fromLive 把 live 记录里的成交转成账本输入。
func fromLive(trade live.Trade, height int64) (Trade, error) {
	amount, err := decimal.NewFromString(trade.BaseAmount)
	if err != nil {
		return Trade{}, fmt.Errorf("trade %s base_amount: %w", trade.TransactionHash, err)
	}
	value, err := parseOptional(trade.ValueUSD)
	if err != nil {
		return Trade{}, fmt.Errorf("trade %s value_usd: %w", trade.TransactionHash, err)
	}
	price, err := parseOptional(trade.PriceUSD)
	if err != nil {
		return Trade{}, fmt.Errorf("trade %s price_usd: %w", trade.TransactionHash, err)
	}
	revision := trade.RevisionHeight
	if revision == 0 {
		revision = height
	}
	return Trade{
		RevisionHeight: revision, BlockHeight: trade.BlockHeight, BlockTime: time.Unix(trade.BlockTime, 0).UTC(),
		TransactionHash: trade.TransactionHash, LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex,
		PoolID: trade.PoolID, Trader: trade.Trader, Side: model.ParseSide(trade.Side), BaseToken: trade.BaseToken,
		QuoteToken: trade.QuoteToken, BaseAmount: amount, PriceUSD: price, ValueUSD: value, Outlier: trade.Outlier,
	}, nil
}

func parseOptional(value string) (decimal.Decimal, error) {
	if value == "" {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(value)
}
