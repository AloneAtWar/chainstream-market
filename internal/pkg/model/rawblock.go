package model

import (
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// NewRawBlock copies block's facts into a RawBlock, lowercasing addresses
// and naming enums by their lowercased suffix. It fails on an amount, answer
// or round that is not an integer, or a negative transfer amount or round.
func NewRawBlock(block *streamv1.BlockEvent) (RawBlock, error) {
	raw := RawBlock{
		ChainID: block.GetChainId(), Height: block.GetBlockHeight(),
		BlockTime: time.Unix(block.GetBlockTime(), 0).UTC(),
	}
	for _, transfer := range block.GetTransfers() {
		amount, ok := new(big.Int).SetString(transfer.GetRawAmount(), 10)
		if !ok || amount.Sign() < 0 {
			return raw, fmt.Errorf("transfer %s log %d: invalid amount %q", transfer.GetTransactionHash(), transfer.GetLogIndex(), transfer.GetRawAmount())
		}
		raw.Transfers = append(raw.Transfers, RawTransfer{
			TransactionHash: transfer.GetTransactionHash(), TransactionIndex: transfer.GetTransactionIndex(),
			LogIndex: transfer.GetLogIndex(), Token: lower(transfer.GetToken()),
			From: lower(transfer.GetFrom()), To: lower(transfer.GetTo()), Amount: amount,
		})
	}
	for _, trade := range block.GetTrades() {
		raw.Trades = append(raw.Trades, newRawTrade(trade))
	}
	for _, update := range block.GetOracleUpdates() {
		answer, answerOK := new(big.Int).SetString(update.GetAnswer(), 10)
		round, roundOK := new(big.Int).SetString(update.GetRoundId(), 10)
		if !answerOK || !roundOK || round.Sign() < 0 {
			return raw, fmt.Errorf("oracle update %s log %d: invalid answer %q or round %q",
				update.GetTransactionHash(), update.GetLogIndex(), update.GetAnswer(), update.GetRoundId())
		}
		raw.Oracles = append(raw.Oracles, RawOracleUpdate{
			TransactionHash: update.GetTransactionHash(), TransactionIndex: update.GetTransactionIndex(),
			LogIndex: update.GetLogIndex(), Feed: lower(update.GetFeed()), Answer: answer, RoundID: round,
			UpdatedAt: update.GetUpdatedAt(),
		})
	}
	for _, fact := range block.GetAssets() {
		deployment := fact.GetDeployment()
		raw.Assets = append(raw.Assets, RawAssetFact{
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
		raw.Pools = append(raw.Pools, RawPoolFact{
			PoolID: lower(fact.GetPoolId()), Protocol: fact.GetProtocol(), DexAddress: lower(fact.GetDexAddress()),
			TokenAddresses: tokens, SourceHeight: fact.GetSourceHeight(), SourceTime: fact.GetSourceTime(),
			Created: fact.GetCreated(), MetadataJSON: string(fact.GetMetadataJson()),
		})
	}
	return raw, nil
}

func newRawTrade(trade *streamv1.Trade) RawTrade {
	raw := RawTrade{
		TransactionHash: trade.GetTransactionHash(), TransactionIndex: trade.GetTransactionIndex(),
		LogIndex: trade.GetLogIndex(), OperationIndex: trade.GetOperationIndex(), Protocol: trade.GetProtocol(),
		DexAddress: lower(trade.GetDexAddress()), PoolID: lower(trade.GetPoolId()),
		Action: enumName(trade.GetAction().String(), "TRADE_ACTION_"),
		Sender: lower(trade.GetSender()), Trader: lower(trade.GetTrader()), Recipient: lower(trade.GetRecipient()),
		Amounts: make([]RawAmount, len(trade.GetAmounts())),
	}
	for index, amount := range trade.GetAmounts() {
		raw.Amounts[index] = RawAmount{
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
