package service

import (
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
)

// The launchpad list defaults: a page of 50, launches of the last day, curves
// that traded in the last hour, and migrations of the last three days.
const (
	launchpadDefaultLimit = 50
	launchpadMaxAge       = 24 * time.Hour
	launchpadStale        = time.Hour
	launchpadMigratedAge  = 72 * time.Hour
	// launchpadMaxWindow bounds every window parameter.
	launchpadMaxWindow = 30 * 24 * time.Hour
)

// progressScale turns a wad progress into a fraction.
var progressScale = decimal.New(1, 18)

// WithLaunchpad serves market-launchpad's lists from reader.
//
// WithLaunchpad 启用发射平台三栏接口。
func (server *Server) WithLaunchpad(reader LaunchpadReader) *Server {
	server.launchpad = reader
	return server
}

// launchpadTokenResponse is one launchpad token. progress is a fraction
// (1 = sold out); base_remaining, quote_raised and price are raw integers,
// price in raw quote units per 1e18 raw token units.
type launchpadTokenResponse struct {
	Token         string  `json:"token"`
	Protocol      string  `json:"protocol"`
	PoolID        string  `json:"pool_id"`
	Manager       string  `json:"manager"`
	Quote         string  `json:"quote"`
	QuoteSymbol   *string `json:"quote_symbol"`
	QuoteDecimals *uint8  `json:"quote_decimals"`
	Creator       *string `json:"creator"`
	Name          *string `json:"name"`
	Symbol        *string `json:"symbol"`
	Decimals      *uint8  `json:"decimals"`
	MetaURI       *string `json:"meta_uri"`
	Status        string  `json:"status"`
	LaunchHeight  int64   `json:"launch_height"`
	LaunchTime    int64   `json:"launch_time"`
	LaunchSeen    bool    `json:"launch_seen"`
	Progress      *string `json:"progress"`
	BaseRemaining *string `json:"base_remaining"`
	QuoteRaised   *string `json:"quote_raised"`
	Price         *string `json:"price"`
	Trades        int64   `json:"trades"`
	Buys          int64   `json:"buys"`
	Sells         int64   `json:"sells"`
	LastTradeTime *int64  `json:"last_trade_time"`
	CompletedTime *int64  `json:"completed_time"`
	MigratedTime  *int64  `json:"migrated_time"`
	TargetPoolID  *string `json:"target_pool_id"`
	PriceUSD      *string `json:"price_usd"`
	Volume24hUSD  *string `json:"volume_24h_usd"`
	Holders       *int64  `json:"holders"`
}

func launchpadToken(listing model.LaunchpadListing) launchpadTokenResponse {
	response := launchpadTokenResponse{
		Token: listing.Token, Protocol: listing.Protocol, PoolID: listing.PoolID, Manager: listing.Manager,
		Quote: listing.Quote, QuoteSymbol: listing.QuoteSymbol, QuoteDecimals: listing.QuoteDecimals, Creator: listing.Creator, Name: listing.Name,
		Symbol: listing.Symbol, Decimals: listing.Decimals, MetaURI: listing.MetaURI, Status: listing.Status,
		LaunchHeight: listing.LaunchHeight, LaunchTime: listing.LaunchTime, LaunchSeen: listing.LaunchSeen,
		BaseRemaining: bigString(listing.BaseRemaining), QuoteRaised: bigString(listing.QuoteRaised),
		Price: bigString(listing.Price), Trades: listing.Trades, Buys: listing.Buys, Sells: listing.Sells,
		LastTradeTime: listing.LastTradeTime, CompletedTime: listing.CompletedTime, MigratedTime: listing.MigratedTime,
		TargetPoolID: listing.TargetPoolID, PriceUSD: listing.PriceUSD, Volume24hUSD: listing.Volume24hUSD,
		Holders: listing.Holders,
	}
	if listing.Progress != nil {
		progress := decimal.NewFromBigInt(listing.Progress, 0).Div(progressScale).String()
		response.Progress = &progress
	}
	return response
}

func bigString(value *big.Int) *string {
	if value == nil {
		return nil
	}
	text := value.String()
	return &text
}

func (server *Server) launchpadList(writer http.ResponseWriter, request *http.Request) {
	query, err := launchpadListQuery(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	listings, err := server.launchpad.LaunchpadList(request.Context(), query)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]launchpadTokenResponse, len(listings))
	var headTime *int64
	for index, listing := range listings {
		body[index] = launchpadToken(listing)
		if headTime == nil && listing.HeadTime != 0 {
			value := listing.HeadTime
			headTime = &value
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"chain_id": query.ChainID, "column": query.Column, "head_time": headTime, "tokens": body,
	})
}

func (server *Server) launchpadDetail(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	token, err := address(request, "token", chain)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	listing, ok, err := server.launchpad.LaunchpadToken(request.Context(), chain, token)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "not a launchpad token"})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"chain_id": chain, "head_time": listing.HeadTime, "token": launchpadToken(listing),
	})
}

func launchpadListQuery(request *http.Request) (model.LaunchpadQuery, error) {
	chain, err := chainID(request)
	if err != nil {
		return model.LaunchpadQuery{}, err
	}
	values := request.URL.Query()
	query := model.LaunchpadQuery{ChainID: chain, Protocol: strings.ToLower(strings.TrimSpace(values.Get("protocol")))}
	switch column := model.LaunchpadColumn(values.Get("column")); column {
	case model.LaunchpadColumnNew, model.LaunchpadColumnBonding, model.LaunchpadColumnMigrated:
		query.Column = column
	default:
		return query, badRequest("column must be new, bonding or migrated")
	}
	for _, window := range []struct {
		name     string
		fallback time.Duration
		target   *int64
	}{
		{"max_age", launchpadMaxAge, &query.MaxAge},
		{"stale", launchpadStale, &query.Stale},
		{"migrated_age", launchpadMigratedAge, &query.MigratedAge},
	} {
		if *window.target, err = windowParam(request, window.name, window.fallback); err != nil {
			return query, err
		}
	}
	if raw := values.Get("min_progress"); raw != "" {
		value, parseErr := decimal.NewFromString(raw)
		if parseErr != nil || value.IsNegative() || value.GreaterThan(decimal.NewFromInt(1)) {
			return query, badRequest("min_progress must be a fraction between 0 and 1")
		}
		query.MinProgress = value.Mul(progressScale).Floor().BigInt()
	}
	query.Limit = launchpadDefaultLimit
	if values.Get("limit") != "" {
		if query.Limit, err = limit(request); err != nil {
			return query, err
		}
	}
	return query, nil
}

// windowParam reads a window as seconds or a Go duration such as 6h.
func windowParam(request *http.Request, name string, fallback time.Duration) (int64, error) {
	raw := strings.TrimSpace(request.URL.Query().Get(name))
	window := fallback
	if raw != "" {
		if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
			window = time.Duration(seconds) * time.Second
		} else if window, err = time.ParseDuration(raw); err != nil {
			return 0, badRequest(name + " must be seconds or a duration such as 6h")
		}
	}
	if window <= 0 || window > launchpadMaxWindow {
		return 0, badRequest(name + " must be positive and at most 720h")
	}
	return int64(window / time.Second), nil
}
