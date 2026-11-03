package data

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/conf"
	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// walletReader 读 market-wallet 的表：PostgreSQL 的持仓和标签，ClickHouse 的流水和统计。
type walletReader struct {
	market *gorm.DB
	state  *gorm.DB
}

// NewWalletReader 返回 market-wallet 表的读取器。
func NewWalletReader(data *Data) service.WalletReader {
	return &walletReader{market: data.market, state: data.state}
}

// walletSorts 把排序名映射到 wallet_stats 上的表达式，都按降序。
var walletSorts = map[string]string{
	"realized": "toFloat64(realized_usd)",
	"winrate":  "if(counted > 0, wins / counted, 0)",
	"roi":      "if(bought_usd > 0, toFloat64(realized_usd) / toFloat64(bought_usd), 0)",
	"volume":   "toFloat64(bought_usd) + toFloat64(sold_usd)",
}

func (reader *walletReader) WalletStats(ctx context.Context, query model.WalletStatsQuery) ([]model.WalletStats, error) {
	refresh, ok, err := chdb.LatestWalletRefresh(ctx, reader.market, query.ChainID)
	if err != nil || !ok {
		return nil, err
	}
	order, known := walletSorts[query.Sort]
	if !known {
		order = walletSorts["realized"]
	}
	db := reader.market.WithContext(ctx).Where("chain_id = ? AND version = ?", query.ChainID, refresh.Version)
	if query.WindowDays > 0 {
		db = db.Where("window_days = ?", query.WindowDays)
	}
	if len(query.Wallets) > 0 {
		db = db.Where("has(?, wallet)", chdb.ArrayParam[string](query.Wallets))
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}
	var rows []chdb.WalletStatsRow
	if err := db.Order(order + " DESC, wallet, window_days").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("clickhouse wallets: stats: %w", err)
	}
	stats := make([]model.WalletStats, len(rows))
	for index, row := range rows {
		stats[index] = row.WalletStats()
	}
	return stats, nil
}

// pricedPositionRow 是持仓加上 market-rankings 的现价和 market-registry 的代币符号。
type pricedPositionRow struct {
	pgdb.PositionRow `gorm:"embedded"`
	PriceUSD         *string `gorm:"column:price_usd"`
	Symbol           *string
}

func (reader *walletReader) Positions(ctx context.Context, query model.PositionQuery) ([]model.PricedPosition, error) {
	db := reader.state.WithContext(ctx).Table("wallet_positions AS p").
		Select(pgdb.PositionColumns+", r.price_usd::text AS price_usd, t.symbol").
		Joins("LEFT JOIN token_rankings r ON r.chain_id = p.chain_id AND r.token = p.token").
		Joins("LEFT JOIN registry_tokens t ON t.chain_id = p.chain_id AND t.address = p.token").
		Where("p.chain_id = ?", int64(query.ChainID))
	switch {
	case query.Wallet != "":
		db = db.Where("p.wallet = ?", query.Wallet).Order("p.last_trade_time DESC, p.token")
	case query.Token != "":
		db = db.Where("p.token = ?", query.Token).Order("p.realized_usd DESC, p.wallet")
	default:
		return nil, fmt.Errorf("postgres wallets: positions need a wallet or a token")
	}
	if query.Open != nil {
		if *query.Open {
			db = db.Where("p.quantity > 0")
		} else {
			db = db.Where("p.quantity = 0")
		}
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}
	var rows []pricedPositionRow
	if err := db.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("postgres wallets: positions: %w", err)
	}
	positions := make([]model.PricedPosition, len(rows))
	for index, row := range rows {
		position, err := row.Position()
		if err != nil {
			return nil, err
		}
		positions[index] = model.PricedPosition{Position: position, Symbol: row.Symbol}
		if row.PriceUSD != nil {
			price, err := decimal.NewFromString(*row.PriceUSD)
			if err != nil {
				return nil, fmt.Errorf("postgres wallets: price of %s: %w", row.Token, err)
			}
			positions[index].PriceUSD = &price
		}
	}
	return positions, nil
}

func (reader *walletReader) WalletTrades(ctx context.Context, query model.WalletTradeQuery) ([]model.WalletTrade, error) {
	db := reader.market.WithContext(ctx).Where("chain_id = ? AND wallet = ?", query.ChainID, query.Wallet)
	if query.Token != "" {
		db = db.Where("token = ?", query.Token)
	}
	if query.BeforeHeight > 0 {
		db = db.Where("block_height < ?", query.BeforeHeight)
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}
	var rows []chdb.WalletTradeRow
	err := db.Order("block_time DESC, block_height DESC, log_index DESC, operation_index DESC").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse wallets: trades: %w", err)
	}
	trades := make([]model.WalletTrade, len(rows))
	for index, row := range rows {
		trades[index] = row.WalletTrade()
	}
	return trades, nil
}

func (reader *walletReader) LabeledWallets(ctx context.Context, chainID uint64, label string) ([]string, error) {
	return pgdb.LabeledWallets(ctx, reader.state, chainID, label)
}

func (reader *walletReader) WalletLabels(ctx context.Context, chainID uint64, wallets []string) (map[string][]string, error) {
	return pgdb.WalletLabels(ctx, reader.state, chainID, wallets)
}

// labelReload 是标签缓存重新读取 wallet_labels 的间隔，和 market-wallet 的汇总间隔相比很短。
const labelReload = time.Minute

// LabelCache 在内存里缓存各链的钱包标签，给交易、持有人和推送频道打标签用。
type LabelCache struct {
	db     *gorm.DB
	chains []uint64
	logger *slog.Logger

	mu     sync.RWMutex
	labels map[uint64]map[string][]string
}

// NewLabelCache 读一次标签，之后每分钟在后台重读；返回的函数停止后台重读。
func NewLabelCache(ctx context.Context, data *Data, bootstrap *conf.Bootstrap, logger *slog.Logger) (*LabelCache, func()) {
	cache := &LabelCache{db: data.state, logger: logger, labels: make(map[uint64]map[string][]string)}
	for _, chain := range bootstrap.GetChains() {
		cache.chains = append(cache.chains, chain.GetChainId())
	}
	cache.reload(ctx)
	background, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(labelReload)
		defer ticker.Stop()
		for {
			select {
			case <-background.Done():
				return
			case <-ticker.C:
				cache.reload(background)
			}
		}
	}()
	return cache, func() {
		cancel()
		<-done
	}
}

func (cache *LabelCache) reload(ctx context.Context) {
	for _, chainID := range cache.chains {
		labels, err := pgdb.AllWalletLabels(ctx, cache.db, chainID)
		if err != nil {
			if ctx.Err() == nil {
				cache.logger.Warn("wallet labels reload failed", "chain_id", chainID, "error", err)
			}
			continue
		}
		cache.mu.Lock()
		cache.labels[chainID] = labels
		cache.mu.Unlock()
	}
}

// WalletLabels 返回钱包的标签，没有时为 nil。返回的切片不能修改。
func (cache *LabelCache) WalletLabels(chainID uint64, wallet string) []string {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.labels[chainID][wallet]
}
