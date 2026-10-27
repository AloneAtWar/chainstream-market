package biz

import (
	"slices"

	"math/big"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

const (
	token = "0x1111111111111111111111111111111111111111"
	alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func transfer(asset, from, to string, amount int64) *streamv1.TokenTransfer {
	return &streamv1.TokenTransfer{Token: asset, From: from, To: to, RawAmount: big.NewInt(amount).String()}
}

func TestNetTransfers(t *testing.T) {
	deltas := netTransfers([]*streamv1.TokenTransfer{
		transfer(token, zeroAddress, "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 10),
		transfer(token, alice, bob, 4),
		transfer(token, bob, alice, 0),
		{Token: token, From: alice, To: bob, RawAmount: "bad"},
	})
	want := map[model.HolderKey]int64{{Token: token, Holder: alice}: 6, {Token: token, Holder: bob}: 4}
	if len(deltas) != len(want) {
		t.Fatalf("deltas = %v", deltas)
	}
	for key, value := range want {
		if deltas[key] == nil || deltas[key].Cmp(big.NewInt(value)) != 0 {
			t.Fatalf("deltas = %v", deltas)
		}
	}
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New() without dependencies error = nil")
	}
}

func TestNativeTouched(t *testing.T) {
	keys := nativeTouched([]string{alice, " 0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "", zeroAddress, bob})
	want := []model.HolderKey{{Token: rpc.NativeToken, Holder: alice}, {Token: rpc.NativeToken, Holder: bob}}
	if !slices.Equal(keys, want) {
		t.Fatalf("keys = %v", keys)
	}
}
