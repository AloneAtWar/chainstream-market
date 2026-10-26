// Package chloader copies a chain topic's raw facts into ClickHouse:
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
package chloader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

type Options struct {
	ChainID uint64
	Store   storage.RawStore
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
	if options.ChainID == 0 || options.Store == nil {
		return nil, errors.New("chloader: chain id and store are required")
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
	checkpoint, ok, err := loader.options.Store.LoadRawCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return chainsync.Checkpoint{}, false, err
	}
	loader.stored = checkpoint.Height
	return checkpoint, true, nil
}

func (loader *Loader) Apply(ctx context.Context, block *streamv1.BlockEvent) error {
	raw, err := convert(block)
	if err != nil {
		return err
	}
	if loader.recovering {
		if err := loader.options.Store.DeleteRawAbove(ctx, loader.options.ChainID, raw.Height-1); err != nil {
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
		if err := loader.options.Store.DeleteRawAbove(ctx, loader.options.ChainID, rollbackTo); err != nil {
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
		if err := loader.options.Store.DeleteRawAbove(ctx, loader.options.ChainID, loader.pending[0].Height-1); err != nil {
			return err
		}
	}
	loader.unsure = true
	if err := loader.options.Store.WriteRaw(ctx, loader.pending); err != nil {
		return err
	}
	if len(loader.pending) > 0 {
		loader.stored = max(loader.stored, loader.pending[len(loader.pending)-1].Height)
	}
	if err := loader.options.Store.SaveRawCheckpoint(ctx, loader.checkpoint); err != nil {
		return err
	}
	if len(loader.pending) > 1 {
		loader.logger.Debug("flushed raw facts", "blocks", len(loader.pending), "rows", loader.rows, "height", loader.checkpoint.Height)
	}
	loader.pending, loader.rows, loader.dirty, loader.unsure = loader.pending[:0], 0, false, false
	loader.lastFlush = loader.now()
	return nil
}

func convert(block *streamv1.BlockEvent) (model.RawBlock, error) {
	raw := model.RawBlock{
		ChainID: block.GetChainId(), Height: block.GetBlockHeight(),
		BlockTime: time.Unix(block.GetBlockTime(), 0).UTC(),
	}
	for _, transfer := range block.GetTransfers() {
		amount, ok := new(big.Int).SetString(transfer.GetRawAmount(), 10)
		if !ok || amount.Sign() < 0 {
			return raw, fmt.Errorf("transfer %s log %d: invalid amount %q", transfer.GetTransactionHash(), transfer.GetLogIndex(), transfer.GetRawAmount())
		}
		raw.Transfers = append(raw.Transfers, model.RawTransfer{
			TransactionHash: transfer.GetTransactionHash(), TransactionIndex: transfer.GetTransactionIndex(),
			LogIndex: transfer.GetLogIndex(), Token: lower(transfer.GetToken()),
			From: lower(transfer.GetFrom()), To: lower(transfer.GetTo()), Amount: amount,
		})
	}
	for _, trade := range block.GetTrades() {
		raw.Trades = append(raw.Trades, convertTrade(trade))
	}
	for _, update := range block.GetOracleUpdates() {
		answer, answerOK := new(big.Int).SetString(update.GetAnswer(), 10)
		round, roundOK := new(big.Int).SetString(update.GetRoundId(), 10)
		if !answerOK || !roundOK || round.Sign() < 0 {
			return raw, fmt.Errorf("oracle update %s log %d: invalid answer %q or round %q",
				update.GetTransactionHash(), update.GetLogIndex(), update.GetAnswer(), update.GetRoundId())
		}
		raw.Oracles = append(raw.Oracles, model.RawOracleUpdate{
			TransactionHash: update.GetTransactionHash(), TransactionIndex: update.GetTransactionIndex(),
			LogIndex: update.GetLogIndex(), Feed: lower(update.GetFeed()), Answer: answer, RoundID: round,
			UpdatedAt: update.GetUpdatedAt(),
		})
	}
	for _, fact := range block.GetAssets() {
		deployment := fact.GetDeployment()
		raw.Assets = append(raw.Assets, model.RawAssetFact{
			Address: lower(fact.GetAddress()), Standard: fact.GetStandard(), Recognition: fact.GetRecognition(),
			MetadataComplete: fact.GetMetadataComplete(), Type: enumName(fact.GetType().String(), "ASSET_FACT_TYPE_"),
			SourceHeight: fact.GetSourceHeight(), DeploymentHeight: deployment.GetBlockHeight(),
			DeploymentTime: deployment.GetBlockTime(), DeploymentTxIdx: deployment.GetTransactionIndex(),
			DeploymentTxHash: deployment.GetTransactionHash(), DeploymentTrace: deployment.GetTraceId(),
			MetadataJSON: string(fact.GetMetadataJson()),
		})
	}
	for _, fact := range block.GetPools() {
		tokens := make([]string, len(fact.GetTokenAddresses()))
		for index, token := range fact.GetTokenAddresses() {
			tokens[index] = lower(token)
		}
		raw.Pools = append(raw.Pools, model.RawPoolFact{
			PoolID: lower(fact.GetPoolId()), Protocol: fact.GetProtocol(), DexAddress: lower(fact.GetDexAddress()),
			TokenAddresses: tokens, SourceHeight: fact.GetSourceHeight(), SourceTime: fact.GetSourceTime(),
			Created: fact.GetCreated(), MetadataJSON: string(fact.GetMetadataJson()),
		})
	}
	return raw, nil
}

func convertTrade(trade *streamv1.Trade) model.RawTrade {
	raw := model.RawTrade{
		TransactionHash: trade.GetTransactionHash(), TransactionIndex: trade.GetTransactionIndex(),
		LogIndex: trade.GetLogIndex(), OperationIndex: trade.GetOperationIndex(), Protocol: trade.GetProtocol(),
		DexAddress: lower(trade.GetDexAddress()), PoolID: lower(trade.GetPoolId()),
		Action: enumName(trade.GetAction().String(), "TRADE_ACTION_"),
		Sender: lower(trade.GetSender()), Trader: lower(trade.GetTrader()), Recipient: lower(trade.GetRecipient()),
		Amounts: make([]model.RawAmount, len(trade.GetAmounts())),
	}
	for index, amount := range trade.GetAmounts() {
		raw.Amounts[index] = model.RawAmount{
			Asset: lower(amount.GetAsset()), Raw: amount.GetRaw(), Value: amount.GetValue(),
			Decimals: amount.GetDecimals(), DecimalsKnown: amount.GetDecimalsKnown(),
		}
	}
	if state := trade.GetPoolState(); state != nil {
		raw.ReservesAfter = slices.Clone(state.GetReservesAfter())
		raw.SqrtPriceX96, raw.Liquidity, raw.Tick = state.GetSqrtPriceX96(), state.GetLiquidity(), state.GetTick()
		if state.ActiveBinId != nil {
			bin := state.GetActiveBinId()
			raw.ActiveBinID = &bin
		}
	}
	return raw
}

// enumName turns TRADE_ACTION_LIQUIDITY_ADDED into liquidity_added.
func enumName(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func lower(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

var _ chainsync.Handler = (*Loader)(nil)
