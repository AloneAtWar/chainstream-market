package service

import (
	"math/big"
	"net/http"
	"strconv"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/feature/dex/mechanism/clamm"
	"github.com/shopspring/decimal"
)

// WithLiquidity serves concentrated-liquidity pools' token amounts per price
// range from reader.
func (server *Server) WithLiquidity(reader LiquidityReader) *Server {
	server.liquidity = reader
	return server
}

// tokenAmount is a raw amount and, when the token's decimals are known, its
// value in token units.
type tokenAmount struct {
	Raw   string  `json:"raw"`
	Value *string `json:"value"`
}

type liquiditySegment struct {
	TickLower int           `json:"tick_lower"`
	TickUpper int           `json:"tick_upper"`
	Liquidity string        `json:"liquidity"`
	Amounts   []tokenAmount `json:"amounts"`
}

type liquidityDepth struct {
	BandBPS int           `json:"band_bps"`
	Amounts []tokenAmount `json:"amounts"`
}

type liquidityResponse struct {
	ChainID        uint64   `json:"chain_id"`
	PoolID         string   `json:"pool_id"`
	Protocol       string   `json:"protocol"`
	Fee            uint32   `json:"fee"`
	SnapshotHeight int64    `json:"snapshot_height"`
	SqrtPriceX96   string   `json:"sqrt_price_x96"`
	Tokens         []string `json:"tokens"`
	// Usable is false when the table no longer bounds the pool: it is stale
	// or the pool's hook may settle swaps outside it.
	Usable         bool               `json:"usable"`
	Stale          bool               `json:"stale"`
	SwapDeltaHooks bool               `json:"swap_delta_hooks"`
	Amounts        []tokenAmount      `json:"amounts"`
	Depth          liquidityDepth     `json:"depth"`
	Segments       []liquiditySegment `json:"segments"`
}

// poolLiquidity returns how many of each token every price range of a
// concentrated pool holds at its current price, their totals (the pool's
// principal, without uncollected fees), and its depth: token0 sold while the
// price rises band_bps and token1 while it falls band_bps. band_bps defaults
// to the pool's fee tier band.
func (server *Server) poolLiquidity(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	poolID, err := pathAddress(request, "pool")
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	table, ok, err := server.liquidity.PoolTickTable(request.Context(), chain, poolID)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "no tick table for pool"})
		return
	}
	band := model.BandBPS(model.DefaultDepthBands(), model.DefaultDynamicFeeDepthBPS, table.Pool.Fee)
	if raw := request.URL.Query().Get("band_bps"); raw != "" {
		band, err = strconv.Atoi(raw)
		if err != nil || band <= 0 || band >= 10_000 {
			server.fail(writer, request, badRequest("band_bps must be between 1 and 9999"))
			return
		}
	}
	price, ok := new(big.Int).SetString(table.Pool.SqrtPriceX96, 10)
	if !ok {
		server.fail(writer, request, badRequest("pool has no price"))
		return
	}
	distribution, err := clamm.SegmentAmounts(table.Ticks, price)
	if err != nil {
		writeJSON(writer, http.StatusConflict, map[string]string{"error": "tick table is inconsistent: " + err.Error()})
		return
	}
	depth0, depth1, err := clamm.DepthWithin(distribution, price, band)
	if err != nil {
		writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	tokens, decimals := server.poolDecimals(request, chain, poolID)
	amounts := func(amount0, amount1 *big.Int) []tokenAmount {
		return []tokenAmount{newTokenAmount(amount0, decimals[0]), newTokenAmount(amount1, decimals[1])}
	}
	body := liquidityResponse{
		ChainID: chain, PoolID: poolID, Protocol: table.Pool.Protocol, Fee: table.Pool.Fee,
		SnapshotHeight: table.Pool.SnapshotHeight, SqrtPriceX96: table.Pool.SqrtPriceX96, Tokens: tokens,
		Usable: table.Pool.Usable(), Stale: table.Pool.Stale, SwapDeltaHooks: table.Pool.SwapDeltaHooks,
		Amounts:  amounts(distribution.Amount0, distribution.Amount1),
		Depth:    liquidityDepth{BandBPS: band, Amounts: amounts(depth0, depth1)},
		Segments: make([]liquiditySegment, len(distribution.Segments)),
	}
	for index, segment := range distribution.Segments {
		body.Segments[index] = liquiditySegment{
			TickLower: segment.TickLower, TickUpper: segment.TickUpper, Liquidity: segment.Liquidity.String(),
			Amounts: amounts(segment.Amount0, segment.Amount1),
		}
	}
	writeJSON(writer, http.StatusOK, body)
}

// poolDecimals returns the pool's token addresses and decimals from the
// registry, as far as it knows them.
func (server *Server) poolDecimals(request *http.Request, chain uint64, poolID string) ([]string, [2]*uint8) {
	var decimals [2]*uint8
	if server.registry == nil {
		return nil, decimals
	}
	pool, ok, err := server.registry.Pool(request.Context(), chain, poolID)
	if err != nil || !ok || len(pool.Tokens) != 2 {
		return nil, decimals
	}
	for index, address := range pool.Tokens {
		if token, ok, err := server.registry.Token(request.Context(), chain, address); err == nil && ok {
			decimals[index] = token.Decimals
		}
	}
	return pool.Tokens, decimals
}

func newTokenAmount(raw *big.Int, decimals *uint8) tokenAmount {
	amount := tokenAmount{Raw: raw.String()}
	if decimals != nil {
		value := decimal.NewFromBigInt(raw, -int32(*decimals)).String()
		amount.Value = &value
	}
	return amount
}
