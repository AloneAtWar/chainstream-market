package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/shopspring/decimal"
)

// maxOffset bounds how deep a token list pages, since each page re-reads
// the rows before it.
const maxOffset = 10000

// WithRankings serves token lists from reader.
func (server *Server) WithRankings(reader storage.RankingReader) *Server {
	server.rankings = reader
	return server
}

type rankedTokenResponse struct {
	Token          string  `json:"token"`
	Name           *string `json:"name"`
	Symbol         *string `json:"symbol"`
	Decimals       *uint8  `json:"decimals"`
	Ranked         bool    `json:"ranked"`
	PriceUSD       *string `json:"price_usd"`
	PriceHeight    *int64  `json:"price_height"`
	PriceChange24h string  `json:"price_change_24h"`
	Volume24hUSD   string  `json:"volume_24h_usd"`
	Trades24h      int64   `json:"trades_24h"`
	LiquidityUSD   string  `json:"liquidity_usd"`
	Holders        int64   `json:"holders"`
	CreatedHeight  *int64  `json:"created_height"`
	CreatedTime    *int64  `json:"created_time"`
}

func (server *Server) tokenList(writer http.ResponseWriter, request *http.Request) {
	query, err := tokenListQuery(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	tokens, err := server.rankings.TokenList(request.Context(), query)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]rankedTokenResponse, len(tokens))
	var refreshedAt *int64
	for index, token := range tokens {
		body[index] = rankedTokenResponse{
			Token: token.Token, Name: token.Name, Symbol: token.Symbol, Decimals: token.Decimals, Ranked: token.Ranked,
			PriceHeight: token.PriceHeight, PriceChange24h: token.PriceChange24h.String(),
			Volume24hUSD: token.Volume24hUSD.String(), Trades24h: token.Trades24h,
			LiquidityUSD: token.LiquidityUSD.String(), Holders: token.Holders,
			CreatedHeight: token.CreatedHeight, CreatedTime: token.CreatedTime,
		}
		if token.PriceUSD != nil {
			price := token.PriceUSD.String()
			body[index].PriceUSD = &price
		}
		if refreshedAt == nil && !token.RefreshedAt.IsZero() {
			unix := token.RefreshedAt.Unix()
			refreshedAt = &unix
		}
	}
	order := "desc"
	if query.Ascending {
		order = "asc"
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"chain_id": query.ChainID, "sort": query.Sort.String(), "order": order,
		"refreshed_at": refreshedAt, "tokens": body,
	})
}

func tokenListQuery(request *http.Request) (model.TokenListQuery, error) {
	chain, err := chainID(request)
	if err != nil {
		return model.TokenListQuery{}, err
	}
	values := request.URL.Query()
	query := model.TokenListQuery{ChainID: chain, Search: strings.TrimSpace(values.Get("q"))}
	if raw := values.Get("sort"); raw != "" {
		sort, ok := model.ParseRankingSort(raw)
		if !ok {
			return query, badRequest("sort must be one of " + strings.Join(model.RankingSorts(), ", "))
		}
		query.Sort = sort
	}
	switch values.Get("order") {
	case "", "desc":
	case "asc":
		query.Ascending = true
	default:
		return query, badRequest("order must be asc or desc")
	}
	if raw := values.Get("min_liquidity_usd"); raw != "" {
		value, parseErr := decimal.NewFromString(raw)
		if parseErr != nil || value.IsNegative() {
			return query, badRequest("min_liquidity_usd must be a non-negative number")
		}
		query.MinLiquidityUSD = value
	}
	if query.Limit, err = limit(request); err != nil {
		return query, err
	}
	if raw := values.Get("offset"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 0 || value > maxOffset {
			return query, badRequest("offset must be between 0 and " + strconv.Itoa(maxOffset))
		}
		query.Offset = value
	}
	return query, nil
}
