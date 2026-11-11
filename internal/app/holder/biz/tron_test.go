package biz

import (
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// TRON base58 addresses are case sensitive, and TRC-20 mints come from the
// base58 zero address, which is not a holder.
func TestNetTransfersKeepTronAddresses(t *testing.T) {
	const (
		usdt  = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
		alice = "TJCnKsPa7y5okkXvQAidZBzqx3QyQ6sxMW"
	)
	deltas := netTransfers([]*streamv1.TokenTransfer{
		{Token: usdt, From: tronZeroAddress, To: alice, RawAmount: "5"},
		{Token: "0xAbC0000000000000000000000000000000000001", From: zeroAddress, To: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", RawAmount: "1"},
	})
	if len(deltas) != 2 {
		t.Fatalf("deltas = %v, want alice and the hex holder only", deltas)
	}
	if delta := deltas[model.HolderKey{Token: usdt, Holder: alice}]; delta == nil || delta.Int64() != 5 {
		t.Fatalf("TRON delta = %v, want 5 under the base58 key", delta)
	}
	hex := model.HolderKey{Token: "0xabc0000000000000000000000000000000000001", Holder: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if delta := deltas[hex]; delta == nil || delta.Int64() != 1 {
		t.Fatalf("hex delta = %v, want 1 under the lowercase key", delta)
	}
}
