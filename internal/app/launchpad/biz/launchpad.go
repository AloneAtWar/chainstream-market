// Package biz is market-launchpad's business layer: it follows launchpad
// bonding curves (four.meme, Flap) through their lifecycle from a chain
// topic's launchpad events and curve trades.
//
// Each block becomes one change per token it touched: the launch with its
// creator, name and symbol; the curve state after the token's last curve trade
// and its trade counts; and whether the curve sold out or moved to a DEX. The
// store merges the change into the token's row, keeping the furthest status,
// and records the row as it was before so a reorg restores it.
//
// market-launchpad 业务层：根据链 topic 的 launchpad_events 和曲线交易（pool_state.curve）
// 跟踪发射平台代币的生命周期。每个区块对每个被触及的代币产出一条改动：发射信息、块内最后一笔
// 曲线交易后的曲线状态与成交笔数、是否打满或迁移。存储层把改动合并进代币行（状态只前进不后退），
// 并在首次修改前保存旧值，重组时据此恢复。
package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
//
// ErrReorgTooDeep 表示回滚超出了保留的撤销历史。
var ErrReorgTooDeep = errors.New("launchpad: reorg deeper than retained history")

// progressOne is a full curve: progress is a wad.
var progressOne = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// LaunchpadRepo persists launchpad tokens. CommitLaunchpad applies a change
// and saves the checkpoint atomically.
//
// LaunchpadRepo 持久化发射平台代币，CommitLaunchpad 原子地写入改动与检查点。
type LaunchpadRepo interface {
	// LoadLaunchpadCheckpoint returns the last committed checkpoint and the
	// highest height whose undo history was pruned.
	LoadLaunchpadCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error)
	CommitLaunchpad(ctx context.Context, checkpoint chainsync.Checkpoint, change model.LaunchpadChange) error
}

// Options configures one chain's Launchpad handler.
//
// Options 配置一条链的 Launchpad handler。
type Options struct {
	ChainID uint64
	Repo    LaunchpadRepo
	// Protocols are the launchpad protocols whose trades count as curve
	// trades even when they carry no curve state.
	Protocols []string
	// HistoryDepth is how many blocks of undo history to keep in addition
	// to everything above the finalized height.
	HistoryDepth int64
	Logger       *slog.Logger
}

// Launchpad is a chainsync.Handler for one chain. It is not safe for
// concurrent use.
//
// Launchpad 是一条链的 chainsync.Handler，不能并发使用。
type Launchpad struct {
	options       Options
	protocols     map[string]struct{}
	logger        *slog.Logger
	pending       model.LaunchpadChange
	prunedThrough int64
}

// New returns one chain's handler.
//
// New 创建一条链的 handler。
func New(options Options) (*Launchpad, error) {
	if options.ChainID == 0 || options.Repo == nil {
		return nil, errors.New("launchpad: chain id and repo are required")
	}
	if options.HistoryDepth <= 0 {
		return nil, errors.New("launchpad: history depth must be positive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	protocols := make(map[string]struct{}, len(options.Protocols))
	for _, protocol := range options.Protocols {
		protocols[strings.ToLower(protocol)] = struct{}{}
	}
	return &Launchpad{options: options, protocols: protocols, logger: logger}, nil
}

// Restore loads the last committed checkpoint.
//
// Restore 读取上次提交的检查点。
func (launchpad *Launchpad) Restore(ctx context.Context, chainID uint64) (chainsync.Checkpoint, bool, error) {
	checkpoint, prunedThrough, ok, err := launchpad.options.Repo.LoadLaunchpadCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	launchpad.prunedThrough = prunedThrough
	return checkpoint, true, nil
}

// Apply turns a block's launchpad events and curve trades into one change
// per token.
//
// Apply 把区块的 launchpad_events 与曲线交易汇总为每个代币一条改动。
func (launchpad *Launchpad) Apply(_ context.Context, block *streamv1.BlockEvent) error {
	height := block.GetBlockHeight()
	collector := newCollector()
	for _, event := range block.GetLaunchpadEvents() {
		launchpad.applyEvent(collector, event)
	}
	for _, trade := range block.GetTrades() {
		launchpad.applyTrade(collector, trade, block.GetBlockTime())
	}
	change := model.LaunchpadChange{Applied: true, Height: height, Time: block.GetBlockTime(), Tokens: collector.tokens()}
	for index := range change.Tokens {
		token := &change.Tokens[index]
		if token.Status != model.LaunchpadBonding && token.Progress == nil {
			token.Progress = new(big.Int).Set(progressOne)
		}
	}
	launchpad.prunedThrough = max(launchpad.prunedThrough, pruneBound(height, block.GetFinalizedHeight(), launchpad.options.HistoryDepth))
	launchpad.pending = change
	return nil
}

func (launchpad *Launchpad) applyEvent(collector *collector, event *streamv1.LaunchpadEvent) {
	token := normalize(event.GetToken())
	if token == "" || event.GetPoolId() == "" {
		return
	}
	entry := collector.get(token, event.GetProtocol(), event.GetPoolId(), event.GetManager(), event.GetQuote())
	switch event.GetType() {
	case streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_CREATED:
		entry.Launched = true
		entry.Creator, entry.Name, entry.Symbol = normalize(event.GetCreator()), clean(event.GetName()), clean(event.GetSymbol())
		entry.MetaURI, entry.LaunchTx = clean(event.GetMetaUri()), normalize(event.GetTransactionHash())
	case streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_COMPLETED:
		entry.Status = furthest(entry.Status, model.LaunchpadCompleted)
	case streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_MIGRATED:
		entry.Status = furthest(entry.Status, model.LaunchpadMigrated)
		entry.TargetPoolID = normalize(event.GetTargetPoolId())
	case streamv1.LaunchpadEventType_LAUNCHPAD_EVENT_TYPE_UNSPECIFIED:
		launchpad.logger.Warn("ignoring launchpad event without a type", "token", token)
	}
}

// applyTrade counts a curve trade: one with a curve state, or any trade of a
// configured launchpad protocol. Its amounts are [token, quote], so the
// token leaving the curve is a buy.
func (launchpad *Launchpad) applyTrade(collector *collector, trade *streamv1.Trade, blockTime int64) {
	curve := trade.GetPoolState().GetCurve()
	if _, launchpadProtocol := launchpad.protocols[strings.ToLower(trade.GetProtocol())]; curve == nil && !launchpadProtocol {
		return
	}
	amounts := trade.GetAmounts()
	manager, _, scoped := strings.Cut(trade.GetPoolId(), ":")
	if trade.GetAction() != streamv1.TradeAction_TRADE_ACTION_SWAP || len(amounts) != 2 || !scoped {
		return
	}
	entry := collector.get(normalize(amounts[0].GetAsset()), trade.GetProtocol(), trade.GetPoolId(), manager, amounts[1].GetAsset())
	entry.Trades++
	if delta, ok := new(big.Int).SetString(amounts[0].GetRaw(), 10); ok && delta.Sign() < 0 {
		entry.Buys++
	} else {
		entry.Sells++
	}
	entry.LastTradeTime = blockTime
	if curve == nil {
		return
	}
	for _, field := range []struct {
		raw    string
		target **big.Int
	}{
		{curve.GetProgress(), &entry.Progress}, {curve.GetBaseRemaining(), &entry.BaseRemaining},
		{curve.GetQuoteRaised(), &entry.QuoteRaised}, {curve.GetPrice(), &entry.Price},
	} {
		if value, ok := new(big.Int).SetString(field.raw, 10); ok && value.Sign() >= 0 {
			*field.target = value
		}
	}
}

// pruneBound is the highest height whose undo history can go: it keeps the
// latest depth blocks and every block above the finalized height, whichever
// reaches further back. An unknown finalized height (0) leaves only the depth.
//
// pruneBound：可丢弃撤销历史的最高高度，同时保留最近 depth 个区块和 finalized 以上的全部区块，
// 取两者中更保守的一个；finalized 未知（0）时只按 depth 保留。
func pruneBound(height, finalized, depth int64) int64 {
	through := height - depth
	if finalized > 0 && finalized < through {
		through = finalized
	}
	return through
}

// Revert discards the pending change; the store restores rows on commit.
//
// Revert 丢弃待提交改动，提交时由存储层按撤销记录恢复。
func (launchpad *Launchpad) Revert(_ context.Context, rollbackTo int64) error {
	if rollbackTo < launchpad.prunedThrough {
		return fmt.Errorf("%w: rollback to %d, history starts after %d", ErrReorgTooDeep, rollbackTo, launchpad.prunedThrough)
	}
	launchpad.pending = model.LaunchpadChange{Reverted: true, RevertTo: rollbackTo}
	return nil
}

// Commit stores the pending change with the checkpoint.
//
// Commit 把待提交改动与检查点一起写入存储。
func (launchpad *Launchpad) Commit(ctx context.Context, checkpoint chainsync.Checkpoint) error {
	change := launchpad.pending
	change.PrunedThrough = launchpad.prunedThrough
	if err := launchpad.options.Repo.CommitLaunchpad(ctx, checkpoint, change); err != nil {
		return err
	}
	launchpad.pending = model.LaunchpadChange{}
	return nil
}

// collector keeps one change per token in the order tokens first appear.
type collector struct {
	byToken map[string]int
	list    []model.LaunchpadToken
}

func newCollector() *collector { return &collector{byToken: make(map[string]int)} }

func (collector *collector) get(token, protocol, poolID, manager, quote string) *model.LaunchpadToken {
	if index, ok := collector.byToken[token]; ok {
		return &collector.list[index]
	}
	collector.byToken[token] = len(collector.list)
	collector.list = append(collector.list, model.LaunchpadToken{
		Token: token, Protocol: strings.ToLower(protocol), PoolID: normalize(poolID),
		Manager: normalize(manager), Quote: normalize(quote), Status: model.LaunchpadBonding,
	})
	return &collector.list[len(collector.list)-1]
}

func (collector *collector) tokens() []model.LaunchpadToken { return collector.list }

var statusRank = map[string]int{model.LaunchpadBonding: 0, model.LaunchpadCompleted: 1, model.LaunchpadMigrated: 2}

// furthest returns the later of two statuses.
func furthest(current, next string) string {
	if statusRank[next] > statusRank[current] {
		return next
	}
	return current
}

func normalize(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

// clean strips NUL characters, which PostgreSQL rejects in text.
func clean(value string) string { return strings.ReplaceAll(value, "\x00", "") }

var _ chainsync.Handler = (*Launchpad)(nil)
