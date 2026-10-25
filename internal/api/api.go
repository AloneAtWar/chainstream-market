// Package api serves stored market data over HTTP.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/live"
	"github.com/AloneAtWar/chainstream-market/internal/market"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

const (
	defaultLimit = 200
	maxLimit     = 1000
)

// Server routes market queries to a storage.Reader.
type Server struct {
	reader    storage.Reader
	holders   storage.HolderReader
	transfers storage.TransferReader
	registry  storage.RegistryReader
	rankings  storage.RankingReader
	live      http.Handler
	intervals map[string]time.Duration
	logger    *slog.Logger
	now       func() time.Time
}

// New serves the given candle intervals, named like "1m", "4h" or "1d".
func New(reader storage.Reader, intervals []time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	named := make(map[string]time.Duration, len(intervals))
	for _, interval := range intervals {
		named[IntervalName(interval)] = interval
	}
	return &Server{reader: reader, intervals: named, logger: logger, now: time.Now}
}

// IntervalName formats an interval the way the API accepts it.
func IntervalName(interval time.Duration) string {
	return live.IntervalName(interval)
}

// WithLive serves the live WebSocket stream at /v1/stream.
func (server *Server) WithLive(stream http.Handler) *Server {
	server.live = stream
	return server
}

func (server *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/price", server.price)
	mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/candles", server.candles(storage.CandleSubjectToken, "token"))
	mux.HandleFunc("GET /v1/chains/{chain}/pools/{pool}/candles", server.candles(storage.CandleSubjectPool, "pool"))
	mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/trades", server.trades("token"))
	mux.HandleFunc("GET /v1/chains/{chain}/pools/{pool}/trades", server.trades("pool"))
	if server.holders != nil {
		mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/holders", server.tokenHolders)
	}
	if server.registry != nil {
		mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}", server.token)
		mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/pools", server.tokenPools)
		mux.HandleFunc("GET /v1/chains/{chain}/pools/{pool}", server.pool)
	}
	if server.rankings != nil {
		mux.HandleFunc("GET /v1/chains/{chain}/tokens", server.tokenList)
	}
	if server.live != nil {
		mux.Handle("GET /v1/stream", server.live)
	}
	if server.transfers != nil {
		mux.HandleFunc("GET /v1/chains/{chain}/tokens/{token}/transfers", server.transfersBy("token"))
		mux.HandleFunc("GET /v1/chains/{chain}/holders/{holder}/transfers", server.transfersBy("holder"))
	}
	return mux
}

type requestError struct{ message string }

func (err requestError) Error() string { return err.message }

func badRequest(message string) error { return requestError{message: message} }

func (server *Server) fail(writer http.ResponseWriter, request *http.Request, err error) {
	var invalid requestError
	if errors.As(err, &invalid) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": invalid.message})
		return
	}
	server.logger.Error("api request failed", "path", request.URL.Path, "error", err)
	writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": "internal error"})
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body) //nolint:errcheck // the client went away
}

func chainID(request *http.Request) (uint64, error) {
	value, err := strconv.ParseUint(request.PathValue("chain"), 10, 64)
	if err != nil || value == 0 {
		return 0, badRequest("chain must be a positive chain id")
	}
	return value, nil
}

// address normalizes a path address; tokens also map aliases such as the
// native pseudo-address to the token that carries their price.
func address(request *http.Request, name string, chain uint64) (string, error) {
	value := strings.ToLower(strings.TrimSpace(request.PathValue(name)))
	if value == "" {
		return "", badRequest(name + " is required")
	}
	if name == "token" {
		value = market.DefaultCatalog(chain).Canonical(value)
	}
	return value, nil
}

func limit(request *http.Request) (int, error) {
	raw := request.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maxLimit {
		return 0, badRequest("limit must be between 1 and " + strconv.Itoa(maxLimit))
	}
	return value, nil
}

func unixParam(request *http.Request, name string, fallback time.Time) (time.Time, error) {
	raw := request.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return time.Time{}, badRequest(name + " must be unix seconds")
	}
	return time.Unix(value, 0).UTC(), nil
}

type priceResponse struct {
	ChainID      uint64 `json:"chain_id"`
	Token        string `json:"token"`
	PriceUSD     string `json:"price_usd"`
	Source       string `json:"source"`
	Hops         uint8  `json:"hops"`
	LiquidityUSD string `json:"liquidity_usd"`
	BlockHeight  int64  `json:"block_height"`
	BlockTime    int64  `json:"block_time"`
}

func (server *Server) price(writer http.ResponseWriter, request *http.Request) {
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
	price, ok, err := server.reader.LatestPrice(request.Context(), chain, token)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "no price for token"})
		return
	}
	writeJSON(writer, http.StatusOK, priceResponse{
		ChainID: chain, Token: token, PriceUSD: price.PriceUSD.String(), Source: price.Source.String(),
		Hops: price.Hops, LiquidityUSD: price.LiquidityUSD.String(),
		BlockHeight: price.BlockHeight, BlockTime: price.BlockTime.Unix(),
	})
}

type candleResponse struct {
	OpenTime   int64  `json:"open_time"`
	Open       string `json:"open"`
	High       string `json:"high"`
	Low        string `json:"low"`
	Close      string `json:"close"`
	VolumeBase string `json:"volume_base"`
	VolumeUSD  string `json:"volume_usd"`
	Trades     uint32 `json:"trades"`
}

func (server *Server) candles(subject storage.CandleSubject, keyName string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		query, err := server.candleQuery(request, subject, keyName)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		candles, err := server.reader.Candles(request.Context(), query)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		body := make([]candleResponse, len(candles))
		for index, candle := range candles {
			body[index] = candleResponse{
				OpenTime: candle.OpenTime.Unix(), Open: candle.Open.String(), High: candle.High.String(),
				Low: candle.Low.String(), Close: candle.Close.String(), VolumeBase: candle.VolumeBase.String(),
				VolumeUSD: candle.VolumeUSD.String(), Trades: candle.Trades,
			}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"interval": IntervalName(query.Interval), "candles": body})
	}
}

func (server *Server) candleQuery(request *http.Request, subject storage.CandleSubject, keyName string) (storage.CandleQuery, error) {
	chain, err := chainID(request)
	if err != nil {
		return storage.CandleQuery{}, err
	}
	key, err := address(request, keyName, chain)
	if err != nil {
		return storage.CandleQuery{}, err
	}
	interval, ok := server.intervals[request.URL.Query().Get("interval")]
	if !ok {
		names := make([]string, 0, len(server.intervals))
		for name := range server.intervals {
			names = append(names, name)
		}
		slices.Sort(names)
		return storage.CandleQuery{}, badRequest("interval must be one of " + strings.Join(names, ", "))
	}
	count, err := limit(request)
	if err != nil {
		return storage.CandleQuery{}, err
	}
	to, err := unixParam(request, "to", server.now().UTC().Add(interval))
	if err != nil {
		return storage.CandleQuery{}, err
	}
	from, err := unixParam(request, "from", time.Unix(0, 0).UTC())
	if err != nil {
		return storage.CandleQuery{}, err
	}
	if !from.Before(to) {
		return storage.CandleQuery{}, badRequest("from must be before to")
	}
	return storage.CandleQuery{
		ChainID: chain, Subject: subject, Key: key, Interval: interval, From: from, To: to, Limit: count,
	}, nil
}

type tradeResponse struct {
	BlockHeight     int64  `json:"block_height"`
	BlockTime       int64  `json:"block_time"`
	TransactionHash string `json:"transaction_hash"`
	LogIndex        uint32 `json:"log_index"`
	OperationIndex  uint32 `json:"operation_index"`
	Protocol        string `json:"protocol"`
	PoolID          string `json:"pool_id"`
	Trader          string `json:"trader"`
	Side            string `json:"side"`
	BaseToken       string `json:"base_token"`
	QuoteToken      string `json:"quote_token"`
	BaseAmount      string `json:"base_amount"`
	QuoteAmount     string `json:"quote_amount"`
	PriceQuote      string `json:"price_quote"`
	PriceUSD        string `json:"price_usd"`
	ValueUSD        string `json:"value_usd"`
	// Outlier marks a swap left out of the token's candles because its
	// price strayed too far from the token's reference price.
	Outlier bool `json:"outlier"`
}

func (server *Server) trades(keyName string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		chain, err := chainID(request)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		key, err := address(request, keyName, chain)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		count, err := limit(request)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		query := storage.TradeQuery{ChainID: chain, Limit: count}
		if keyName == "token" {
			query.BaseToken = key
		} else {
			query.PoolID = key
		}
		if raw := request.URL.Query().Get("before_height"); raw != "" {
			height, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil || height <= 0 {
				server.fail(writer, request, badRequest("before_height must be a positive height"))
				return
			}
			query.BeforeHeight = height
		}
		trades, err := server.reader.Trades(request.Context(), query)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		body := make([]tradeResponse, len(trades))
		for index, trade := range trades {
			body[index] = tradeResponse{
				BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime.Unix(),
				TransactionHash: trade.TransactionHash, LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex,
				Protocol: trade.Protocol, PoolID: trade.PoolID, Trader: trade.Trader, Side: trade.Side.String(),
				BaseToken: trade.BaseToken, QuoteToken: trade.QuoteToken,
				BaseAmount: trade.BaseAmount.String(), QuoteAmount: trade.QuoteAmount.String(),
				PriceQuote: trade.PriceQuote.String(), PriceUSD: trade.PriceUSD.String(), ValueUSD: trade.ValueUSD.String(),
				Outlier: trade.Outlier,
			}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"trades": body})
	}
}
