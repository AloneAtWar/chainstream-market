package service

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/clamm"
)

type fakeLiquidity struct {
	tables map[string]model.PoolTickTable
}

func (fake fakeLiquidity) PoolTickTable(_ context.Context, _ uint64, poolID string) (model.PoolTickTable, bool, error) {
	table, ok := fake.tables[poolID]
	return table, ok, nil
}

func TestPoolLiquidity(t *testing.T) {
	price, _ := clamm.SqrtRatioAtTick(0)
	table := map[int]*big.Int{
		-120: big.NewInt(3_000_000_000), -60: big.NewInt(5_000_000_000),
		60: big.NewInt(-3_000_000_000), 120: big.NewInt(-5_000_000_000),
	}
	liquidity := fakeLiquidity{tables: map[string]model.PoolTickTable{
		"0xcc":  {Pool: model.CLAMMPool{Protocol: "uniswap_v3", Fee: 500, SnapshotHeight: 9, SqrtPriceX96: price.String()}, Ticks: table},
		"0xbad": {Pool: model.CLAMMPool{SqrtPriceX96: price.String()}, Ticks: map[int]*big.Int{-60: big.NewInt(5)}},
	}}
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithRegistry(newFakeRegistry()).WithLiquidity(liquidity)

	code, body := serveRegistry(t, server, "/v1/chains/1/pools/0xCC/liquidity")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	var got liquidityResponse
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want, _ := clamm.SegmentAmounts(table, price)
	depth0, depth1, _ := clamm.DepthWithin(want, price, 100)
	if got.PoolID != "0xcc" || got.Fee != 500 || !got.Usable || len(got.Tokens) != 2 || len(got.Segments) != 3 ||
		got.Amounts[0].Raw != want.Amount0.String() || got.Amounts[1].Raw != want.Amount1.String() ||
		got.Depth.BandBPS != 100 || got.Depth.Amounts[0].Raw != depth0.String() || got.Depth.Amounts[1].Raw != depth1.String() {
		t.Fatalf("response = %s", body)
	}
	// 0xaa has 6 decimals in the registry, 0xbb none.
	if value := got.Amounts[0].Value; value == nil || got.Amounts[1].Value != nil {
		t.Fatalf("amount values = %s", body)
	}
	if segment := got.Segments[1]; segment.TickLower != -60 || segment.TickUpper != 60 || segment.Liquidity != "8000000000" {
		t.Fatalf("middle segment = %+v", segment)
	}

	if code, body := serveRegistry(t, server, "/v1/chains/1/pools/0xcc/liquidity?band_bps=500"); code != http.StatusOK ||
		!json.Valid([]byte(body)) {
		t.Fatalf("band_bps: %d %s", code, body)
	}
	for path, status := range map[string]int{
		"/v1/chains/1/pools/0xcc/liquidity?band_bps=0": http.StatusBadRequest,
		"/v1/chains/1/pools/0xdd/liquidity":            http.StatusNotFound,
		"/v1/chains/1/pools/0xbad/liquidity":           http.StatusConflict,
	} {
		if code, body := serveRegistry(t, server, path); code != status {
			t.Fatalf("%s: status %d, want %d: %s", path, code, status, body)
		}
	}
}
