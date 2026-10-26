package model

import (
	"slices"
	"testing"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

func TestNewRawBlockKeepsPublishedFacts(t *testing.T) {
	bin := uint32(42)
	block := &streamv1.BlockEvent{
		ChainId: 56, BlockHeight: 7, BlockTime: 1_700_000_000,
		Transfers: []*streamv1.TokenTransfer{{Token: "0xT", From: "0xA", To: "0xB", RawAmount: "0", LogIndex: 3}},
		Trades: []*streamv1.Trade{{
			PoolId: "0xP", Action: streamv1.TradeAction_TRADE_ACTION_LIQUIDITY_ADDED, Trader: "0xA",
			Amounts:   []*streamv1.Amount{{Asset: "0xT", Raw: "-10"}},
			PoolState: &streamv1.PoolState{ReservesAfter: []string{"1", "2"}, ActiveBinId: &bin},
		}},
		OracleUpdates: []*streamv1.OracleUpdate{{Feed: "0xF", Answer: "-3", RoundId: "9"}},
		Assets: []*streamv1.AssetFact{{
			Address: "0xT", Type: streamv1.AssetFactType_ASSET_FACT_TYPE_CREATED,
			Deployment: &streamv1.DeploymentRef{BlockHeight: 6, TransactionHash: "0xd"}, MetadataJson: []byte(`{"decimals":18}`),
		}},
		Pools: []*streamv1.PoolFact{{PoolId: "0xP", TokenAddresses: []string{"0xT", "0xW"}, Created: true}},
	}
	raw, err := NewRawBlock(block)
	if err != nil {
		t.Fatalf("NewRawBlock() error = %v", err)
	}
	// Zero-value transfers are kept: every transfer is stored.
	if raw.Rows() != 5 || raw.Transfers[0].Token != "0xt" || raw.Transfers[0].Amount.Sign() != 0 {
		t.Fatalf("raw = %+v", raw)
	}
	trade := raw.Trades[0]
	if trade.Action != "liquidity_added" || trade.PoolID != "0xp" || trade.Amounts[0].Raw != "-10" ||
		!slices.Equal(trade.ReservesAfter, []string{"1", "2"}) || *trade.ActiveBinID != 42 {
		t.Fatalf("trade = %+v", trade)
	}
	if raw.Oracles[0].Answer.Int64() != -3 || raw.Assets[0].Type != "created" || raw.Assets[0].DeploymentHeight != 6 ||
		raw.Assets[0].MetadataJSON != `{"decimals":18}` || raw.Pools[0].TokenAddresses[1] != "0xw" {
		t.Fatalf("facts = %+v %+v %+v", raw.Oracles, raw.Assets, raw.Pools)
	}

	block.Transfers[0].RawAmount = "-1"
	if _, err := NewRawBlock(block); err == nil {
		t.Fatal("NewRawBlock() with a negative transfer error = nil")
	}
}
