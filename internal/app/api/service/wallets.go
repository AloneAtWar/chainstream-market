package service

import (
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

const (
	// defaultWindowDays 是钱包排行默认的统计窗口。
	defaultWindowDays = 30
	// maxWindowDays 是 market-wallet 统计窗口的上限。
	maxWindowDays = 90
)

// WithWallets 用 reader 提供钱包画像、持仓、流水、排行和代币盈利榜。
func (server *Server) WithWallets(reader WalletReader) *Server {
	server.wallets = reader
	return server
}

// WithLabels 给交易和持有人附上钱包标签。
func (server *Server) WithLabels(labels live.LabelSource) *Server {
	server.labels = labels
	return server
}

// walletLabels 从标签缓存返回钱包的标签，没有时为 nil。
func (server *Server) walletLabels(chain uint64, wallet string) []string {
	if server.labels == nil {
		return nil
	}
	return server.labels.WalletLabels(chain, wallet)
}

type roiDistributionResponse struct {
	Over5x     uint64 `json:"over_5x"`
	From2xTo5x uint64 `json:"from_2x_to_5x"`
	From0To2x  uint64 `json:"from_0_to_2x"`
	Loss0To50  uint64 `json:"loss_0_to_50"`
	LossOver50 uint64 `json:"loss_over_50"`
}

type walletStatsResponse struct {
	WindowDays  uint16 `json:"window_days"`
	RealizedUSD string `json:"realized_usd"`
	BoughtUSD   string `json:"bought_usd"`
	SoldUSD     string `json:"sold_usd"`
	// ROI 是已实现盈亏除以窗口内买入金额。
	ROI   string `json:"roi"`
	Buys  uint64 `json:"buys"`
	Sells uint64 `json:"sells"`
	// WinRate 是计入已实现盈亏的卖出里盈利的比例（按笔）。
	WinRate            float64                 `json:"win_rate"`
	Wins               uint64                  `json:"wins"`
	CountedSells       uint64                  `json:"counted_sells"`
	Tokens             uint64                  `json:"tokens"`
	ROIDistribution    roiDistributionResponse `json:"roi_distribution"`
	Closes             uint64                  `json:"closes"`
	AverageHoldSeconds int64                   `json:"average_hold_seconds"`
	FastCloses         uint64                  `json:"fast_closes"`
}

func newWalletStatsResponse(stats model.WalletStats) walletStatsResponse {
	return walletStatsResponse{
		WindowDays: stats.WindowDays, RealizedUSD: stats.RealizedUSD.String(), BoughtUSD: stats.BoughtUSD.String(),
		SoldUSD: stats.SoldUSD.String(), ROI: stats.ROI().String(), Buys: stats.Buys, Sells: stats.Sells,
		WinRate: stats.WinRate(), Wins: stats.Wins, CountedSells: stats.Counted, Tokens: stats.Tokens,
		ROIDistribution: roiDistributionResponse{
			Over5x: stats.TokensOver5x, From2xTo5x: stats.Tokens2xTo5x, From0To2x: stats.Tokens0To2x,
			Loss0To50: stats.TokensLoss50, LossOver50: stats.TokensLossOver,
		},
		Closes: stats.Closes, AverageHoldSeconds: int64(stats.AverageHold() / time.Second), FastCloses: stats.FastCloses,
	}
}

func refreshedAt(stats []model.WalletStats) *int64 {
	for _, entry := range stats {
		if !entry.RefreshedAt.IsZero() {
			unix := entry.RefreshedAt.Unix()
			return &unix
		}
	}
	return nil
}

// wallet 返回一个钱包的标签和各窗口的统计。
func (server *Server) wallet(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	wallet, err := address(request, "wallet", chain)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	stats, err := server.wallets.WalletStats(request.Context(), model.WalletStatsQuery{ChainID: chain, Wallets: []string{wallet}})
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	slices.SortFunc(stats, func(left, right model.WalletStats) int { return int(left.WindowDays) - int(right.WindowDays) })
	body := make([]walletStatsResponse, len(stats))
	for index, entry := range stats {
		body[index] = newWalletStatsResponse(entry)
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"chain_id": chain, "wallet": wallet, "labels": nonNil(server.walletLabels(chain, wallet)),
		"refreshed_at": refreshedAt(stats), "stats": body,
	})
}

type rankedWalletResponse struct {
	Wallet string   `json:"wallet"`
	Labels []string `json:"labels"`
	walletStatsResponse
}

// walletList 返回一个窗口的钱包排行，可按标签过滤（如 label=smart_money）。
func (server *Server) walletList(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	window := defaultWindowDays
	if raw := request.URL.Query().Get("window"); raw != "" {
		window, err = strconv.Atoi(raw)
		if err != nil || window < 1 || window > maxWindowDays {
			server.fail(writer, request, badRequest("window must be days between 1 and "+strconv.Itoa(maxWindowDays)))
			return
		}
	}
	sort := request.URL.Query().Get("sort")
	switch sort {
	case "":
		sort = "realized"
	case "realized", "winrate", "roi", "volume":
	default:
		server.fail(writer, request, badRequest("sort must be one of realized, winrate, roi, volume"))
		return
	}
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	query := model.WalletStatsQuery{ChainID: chain, WindowDays: uint16(window), Sort: sort, Limit: count}
	label := request.URL.Query().Get("label")
	if label != "" {
		query.Wallets, err = server.wallets.LabeledWallets(request.Context(), chain, label)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
	}
	var stats []model.WalletStats
	if label == "" || len(query.Wallets) > 0 {
		stats, err = server.wallets.WalletStats(request.Context(), query)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
	}
	wallets := make([]string, len(stats))
	for index, entry := range stats {
		wallets[index] = entry.Wallet
	}
	labels, err := server.wallets.WalletLabels(request.Context(), chain, wallets)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]rankedWalletResponse, len(stats))
	for index, entry := range stats {
		body[index] = rankedWalletResponse{Wallet: entry.Wallet, Labels: nonNil(labels[entry.Wallet]), walletStatsResponse: newWalletStatsResponse(entry)}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"chain_id": chain, "window_days": window, "sort": sort, "label": label,
		"refreshed_at": refreshedAt(stats), "wallets": body,
	})
}

type positionResponse struct {
	Wallet string  `json:"wallet"`
	Token  string  `json:"token"`
	Symbol *string `json:"symbol"`
	// Status 是 open（仍有有成本的持仓）或 closed。
	Status         string  `json:"status"`
	Quantity       string  `json:"quantity"`
	AverageCostUSD string  `json:"average_cost_usd"`
	CostUSD        string  `json:"cost_usd"`
	PriceUSD       *string `json:"price_usd"`
	RealizedUSD    string  `json:"realized_usd"`
	UnrealizedUSD  *string `json:"unrealized_usd"`
	BoughtQuantity string  `json:"bought_quantity"`
	SoldQuantity   string  `json:"sold_quantity"`
	BoughtUSD      string  `json:"bought_usd"`
	SoldUSD        string  `json:"sold_usd"`
	Buys           int64   `json:"buys"`
	Sells          int64   `json:"sells"`
	Wins           int64   `json:"wins"`
	Losses         int64   `json:"losses"`
	FirstBuyHeight *int64  `json:"first_buy_height"`
	FirstBuyTime   *int64  `json:"first_buy_time"`
	OpenedAt       *int64  `json:"opened_at"`
	LastTradeTime  int64   `json:"last_trade_time"`
}

func newPositionResponse(position model.PricedPosition) positionResponse {
	body := positionResponse{
		Wallet: position.Wallet, Token: position.Token, Symbol: position.Symbol, Status: "closed",
		Quantity: position.Quantity.String(), AverageCostUSD: position.AverageCost().String(),
		CostUSD: position.CostUSD.String(), RealizedUSD: position.RealizedUSD.String(),
		BoughtQuantity: position.BoughtQty.String(), SoldQuantity: position.SoldQty.String(),
		BoughtUSD: position.BoughtUSD.String(), SoldUSD: position.SoldUSD.String(),
		Buys: position.Buys, Sells: position.Sells, Wins: position.Wins, Losses: position.Losses,
		FirstBuyTime: optionalUnix(position.FirstBuyTime), OpenedAt: optionalUnix(position.OpenedAt),
		LastTradeTime: position.LastTradeTime.Unix(),
	}
	if position.Quantity.IsPositive() {
		body.Status = "open"
	}
	if position.FirstBuyHeight > 0 {
		height := position.FirstBuyHeight
		body.FirstBuyHeight = &height
	}
	if position.PriceUSD != nil {
		price := position.PriceUSD.String()
		body.PriceUSD = &price
	}
	if unrealized := position.UnrealizedUSD(); unrealized != nil {
		value := unrealized.String()
		body.UnrealizedUSD = &value
	}
	return body
}

func optionalUnix(value time.Time) *int64 {
	if value.IsZero() {
		return nil
	}
	unix := value.Unix()
	return &unix
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// walletPositions 返回钱包的持仓，status=open 或 closed 过滤，最近交易的在前。
func (server *Server) walletPositions(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	wallet, err := address(request, "wallet", chain)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	query := model.PositionQuery{ChainID: chain, Wallet: wallet, Limit: count}
	switch status := request.URL.Query().Get("status"); status {
	case "":
	case "open", "closed":
		open := status == "open"
		query.Open = &open
	default:
		server.fail(writer, request, badRequest("status must be open or closed"))
		return
	}
	positions, err := server.wallets.Positions(request.Context(), query)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]positionResponse, len(positions))
	for index, position := range positions {
		body[index] = newPositionResponse(position)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"chain_id": chain, "wallet": wallet, "positions": body})
}

type walletTradeResponse struct {
	BlockHeight     int64  `json:"block_height"`
	BlockTime       int64  `json:"block_time"`
	TransactionHash string `json:"transaction_hash"`
	LogIndex        uint32 `json:"log_index"`
	OperationIndex  uint32 `json:"operation_index"`
	PoolID          string `json:"pool_id"`
	Token           string `json:"token"`
	QuoteToken      string `json:"quote_token"`
	Side            string `json:"side"`
	// Event 是 open、add、reduce、close 或 untracked（卖出没有成本的代币）。
	Event       string `json:"event"`
	Amount      string `json:"amount"`
	ValueUSD    string `json:"value_usd"`
	PriceUSD    string `json:"price_usd"`
	RealizedUSD string `json:"realized_usd"`
	// Counted 表示这笔卖出计入已实现盈亏和胜率。
	Counted       bool   `json:"counted"`
	HoldSeconds   int64  `json:"hold_seconds"`
	QuantityAfter string `json:"quantity_after"`
	CostAfter     string `json:"cost_after_usd"`
}

// walletTrades 返回钱包的账本流水，可按 token 过滤，用 before_height 往前翻页。
func (server *Server) walletTrades(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	wallet, err := address(request, "wallet", chain)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	query := model.WalletTradeQuery{ChainID: chain, Wallet: wallet, Limit: count}
	if token := request.URL.Query().Get("token"); token != "" {
		request.SetPathValue("token", token)
		if query.Token, err = address(request, "token", chain); err != nil {
			server.fail(writer, request, err)
			return
		}
	}
	if err = walletTradeCursor(request, &query); err != nil {
		server.fail(writer, request, err)
		return
	}
	trades, err := server.wallets.WalletTrades(request.Context(), query)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]walletTradeResponse, len(trades))
	for index, trade := range trades {
		body[index] = walletTradeResponse{
			BlockHeight: trade.BlockHeight, BlockTime: trade.BlockTime.Unix(), TransactionHash: trade.TransactionHash,
			LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, PoolID: trade.PoolID, Token: trade.Token,
			QuoteToken: trade.QuoteToken, Side: trade.Side.String(), Event: trade.Event.String(),
			Amount: trade.Amount.String(), ValueUSD: trade.ValueUSD.String(), PriceUSD: trade.PriceUSD.String(),
			RealizedUSD: trade.RealizedUSD.String(), Counted: trade.Counted, HoldSeconds: trade.HoldSeconds,
			QuantityAfter: trade.QuantityAfter.String(), CostAfter: trade.CostAfter.String(),
		}
	}
	response := map[string]any{"chain_id": chain, "wallet": wallet, "trades": body}
	if len(trades) == count {
		last := trades[len(trades)-1]
		response["next"] = walletTradeNext{
			BeforeHeight: last.BlockHeight, BeforeLogIndex: last.LogIndex, BeforeOperationIndex: last.OperationIndex,
		}
	}
	writeJSON(writer, http.StatusOK, response)
}

// walletTradeNext 是整页返回时的续页位置：三个值原样作为查询参数传回。
type walletTradeNext struct {
	BeforeHeight         int64  `json:"before_height"`
	BeforeLogIndex       uint32 `json:"before_log_index"`
	BeforeOperationIndex uint32 `json:"before_operation_index"`
}

// walletTradeCursor 读 before_height、before_log_index、before_operation_index；后两个依次
// 需要前一个。
func walletTradeCursor(request *http.Request, query *model.WalletTradeQuery) error {
	values := request.URL.Query()
	if raw := values.Get("before_height"); raw != "" {
		height, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || height <= 0 {
			return badRequest("before_height must be a positive height")
		}
		query.BeforeHeight = height
	}
	if raw := values.Get("before_log_index"); raw != "" {
		index, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || query.BeforeHeight == 0 {
			return badRequest("before_log_index must be a log index and needs before_height")
		}
		logIndex := uint32(index)
		query.BeforeLogIndex = &logIndex
	}
	if raw := values.Get("before_operation_index"); raw != "" {
		index, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || query.BeforeLogIndex == nil {
			return badRequest("before_operation_index must be an operation index and needs before_log_index")
		}
		operation := uint32(index)
		query.BeforeOperationIndex = &operation
	}
	return nil
}

type traderResponse struct {
	Labels []string `json:"labels"`
	positionResponse
}

// tokenTraders 返回一个代币的交易者，按已实现盈亏降序（盈利榜），附上钱包标签。
func (server *Server) tokenTraders(writer http.ResponseWriter, request *http.Request) {
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
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	positions, err := server.wallets.Positions(request.Context(), model.PositionQuery{ChainID: chain, Token: token, Limit: count})
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	wallets := make([]string, len(positions))
	for index, position := range positions {
		wallets[index] = position.Wallet
	}
	labels, err := server.wallets.WalletLabels(request.Context(), chain, wallets)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]traderResponse, len(positions))
	for index, position := range positions {
		body[index] = traderResponse{Labels: nonNil(labels[position.Wallet]), positionResponse: newPositionResponse(position)}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"chain_id": chain, "token": token, "traders": body})
}
