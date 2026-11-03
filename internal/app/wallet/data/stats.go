package data

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// statsRepo 在 ClickHouse 里汇总 wallet_trades。
type statsRepo struct {
	db *gorm.DB
}

// NewStatsRepo 返回汇总用的仓储。
func NewStatsRepo(data *Data) biz.StatsRepo { return &statsRepo{db: data.market} }

func (repo *statsRepo) TradeHead(ctx context.Context, chainID uint64) (time.Time, int64, bool, error) {
	var row struct {
		Head        time.Time
		ApplyHeight int64
		Found       uint64
	}
	err := repo.db.WithContext(ctx).Raw(`SELECT max(block_time) AS head, max(apply_height) AS apply_height, count() AS found
		FROM wallet_trades WHERE chain_id = ?`, chainID).Scan(&row).Error
	if err != nil {
		return time.Time{}, 0, false, fmt.Errorf("clickhouse store: wallet trade head: %w", err)
	}
	return row.Head.UTC(), row.ApplyHeight, row.Found > 0, nil
}

func (repo *statsRepo) LoadRefresh(ctx context.Context, chainID uint64) (biz.Refresh, bool, error) {
	row, ok, err := chdb.LatestWalletRefresh(ctx, repo.db, chainID)
	if err != nil || !ok {
		return biz.Refresh{}, false, err
	}
	return biz.Refresh{
		ChainID: chainID, Version: row.Version, Started: row.Started, Head: row.HeadTime.UTC(), ApplyHeight: row.ApplyHeight,
	}, true, nil
}

func (repo *statsRepo) Invalidations(ctx context.Context, chainID uint64, since uint64) (time.Time, int64, bool, error) {
	var row struct {
		Day    time.Time
		Height int64
		Found  uint64
	}
	err := repo.db.WithContext(ctx).Raw(`SELECT min(day) AS day, min(height) AS height, count() AS found
		FROM wallet_invalidations WHERE chain_id = ? AND at >= ?`, chainID, since).Scan(&row).Error
	if err != nil {
		return time.Time{}, 0, false, fmt.Errorf("clickhouse store: wallet invalidations: %w", err)
	}
	return row.Day.UTC(), row.Height, row.Found > 0, nil
}

func (repo *statsRepo) PruneStats(ctx context.Context, chainID uint64, keep []uint64) error {
	var versions []uint64
	db := repo.db.WithContext(ctx)
	if err := db.Raw(`SELECT DISTINCT version FROM wallet_stats WHERE chain_id = ?`, chainID).Scan(&versions).Error; err != nil {
		return fmt.Errorf("clickhouse store: wallet stats versions: %w", err)
	}
	for _, version := range versions {
		if slices.Contains(keep, version) {
			continue
		}
		// 每个版本一个分区，删分区不用改写数据。
		if err := db.Exec(fmt.Sprintf("ALTER TABLE wallet_stats DROP PARTITION (%d, %d)", chainID, version)).Error; err != nil {
			return fmt.Errorf("clickhouse store: drop wallet stats version %d: %w", version, err)
		}
	}
	return nil
}

func (repo *statsRepo) EarliestTradeFrom(ctx context.Context, chainID uint64, height int64) (time.Time, bool, error) {
	var row struct {
		Earliest time.Time
		Found    uint64
	}
	err := repo.db.WithContext(ctx).Raw(`SELECT min(block_time) AS earliest, count() AS found
		FROM wallet_trades WHERE chain_id = ? AND apply_height >= ?`, chainID, height).Scan(&row).Error
	if err != nil {
		return time.Time{}, false, fmt.Errorf("clickhouse store: earliest wallet trade: %w", err)
	}
	return row.Earliest.UTC(), row.Found > 0, nil
}

// rebuildDaily 把 from 及之后的流水按（天, 钱包, 代币）汇总。胜负只数计入已实现盈亏的
// 卖出；清仓的持仓时长和快进快出只数 close 事件。按列的位置写入，不给汇总列起和源列
// 同名的别名：ClickHouse 的别名在整个查询里可见，会盖住同名的源列。
const rebuildDaily = `INSERT INTO wallet_token_daily
	SELECT chain_id, toDate(block_time) AS day, wallet, token,
		countIf(side = 'buy'),
		countIf(side = 'sell'),
		sumIf(value_usd, side = 'buy'),
		sumIf(value_usd, side = 'sell'),
		sumIf(realized_usd, counted),
		sumIf(cost_sold_usd, counted),
		countIf(counted AND realized_usd > 0),
		countIf(counted),
		countIf(event = 'close'),
		toUInt64(sumIf(greatest(hold_seconds, 0), event = 'close')),
		countIf(event = 'close' AND hold_seconds <= ?)
	FROM wallet_trades
	WHERE chain_id = ? AND block_time >= ?
	GROUP BY chain_id, day, wallet, token`

func (repo *statsRepo) RebuildDaily(ctx context.Context, chainID uint64, from time.Time, fastCloseSeconds int64) error {
	from = from.UTC()
	db := repo.db.WithContext(ctx)
	if err := db.Exec(`DELETE FROM wallet_token_daily WHERE chain_id = ? AND day >= toDate(?)`, chainID, from).Error; err != nil {
		return fmt.Errorf("clickhouse store: clear wallet daily: %w", err)
	}
	if err := db.Exec(rebuildDaily, fastCloseSeconds, chainID, from).Error; err != nil {
		return fmt.Errorf("clickhouse store: rebuild wallet daily: %w", err)
	}
	return nil
}

// writeStats 先按（钱包, 代币）加总窗口内的日汇总，算出每个代币的 ROI（已实现 / 卖出
// 部分成本），再按钱包加总并分桶。没有计入盈亏的卖出的代币 ROI 为 NaN，不进任何桶。
const writeStats = `INSERT INTO wallet_stats
	SELECT ? AS chain_id, ? AS window_days, wallet,
		sum(t_realized), sum(t_bought), sum(t_sold), sum(t_buys), sum(t_sells), sum(t_wins), sum(t_counted),
		count(),
		countIf(t_roi > 5),
		countIf(t_roi > 2 AND t_roi <= 5),
		countIf(t_roi >= 0 AND t_roi <= 2),
		countIf(t_roi >= -0.5 AND t_roi < 0),
		countIf(t_roi < -0.5),
		sum(t_closes), sum(t_hold), sum(t_fast),
		?
	FROM (
		SELECT wallet, token,
			sum(realized_usd) AS t_realized, sum(cost_sold_usd) AS t_cost_sold,
			sum(bought_usd) AS t_bought, sum(sold_usd) AS t_sold,
			sum(buys) AS t_buys, sum(sells) AS t_sells, sum(wins) AS t_wins, sum(counted) AS t_counted,
			sum(closes) AS t_closes, sum(hold_seconds) AS t_hold, sum(fast_closes) AS t_fast,
			if(t_cost_sold > 0, toFloat64(t_realized) / toFloat64(t_cost_sold), nan) AS t_roi
		FROM wallet_token_daily
		WHERE chain_id = ? AND day >= toDate(?)
		GROUP BY wallet, token
	)
	GROUP BY wallet`

func (repo *statsRepo) WriteStats(ctx context.Context, chainID uint64, windowDays int, from time.Time, version uint64) error {
	err := repo.db.WithContext(ctx).Exec(writeStats, chainID, uint16(windowDays), version, chainID, from.UTC()).Error
	if err != nil {
		return fmt.Errorf("clickhouse store: write wallet stats %dd: %w", windowDays, err)
	}
	return nil
}

// botCondition 是机器人规则在 wallet_stats 上的条件，参数依次是最少交易笔数、最少清仓
// 次数和快进快出占比。
const botCondition = `(buys + sells >= ? OR (closes >= ? AND fast_closes >= closes * ?))`

func botArgs(rule biz.BotRule) []any {
	return []any{rule.MinTrades, rule.MinCloses, rule.MinFastCloseShare}
}

func (repo *statsRepo) Bots(ctx context.Context, chainID uint64, rule biz.BotRule, version uint64) ([]string, error) {
	var wallets []string
	args := append([]any{chainID, uint16(rule.WindowDays), version}, botArgs(rule)...)
	err := repo.db.WithContext(ctx).Raw(`SELECT wallet FROM wallet_stats
		WHERE chain_id = ? AND window_days = ? AND version = ? AND `+botCondition+`
		ORDER BY wallet`, args...).Scan(&wallets).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: bot wallets: %w", err)
	}
	return wallets, nil
}

func (repo *statsRepo) SmartMoney(ctx context.Context, chainID uint64, rule biz.SmartMoneyRule, bot biz.BotRule, version uint64) ([]string, error) {
	var wallets []string
	args := []any{chainID, uint16(rule.WindowDays), version, rule.MinRealizedUSD, rule.MinWinRate, rule.MinTokens, rule.MaxLossShare,
		chainID, uint16(bot.WindowDays), version}
	args = append(args, botArgs(bot)...)
	args = append(args, rule.MaxWallets)
	err := repo.db.WithContext(ctx).Raw(`SELECT wallet FROM wallet_stats
		WHERE chain_id = ? AND window_days = ? AND version = ?
			AND toFloat64(realized_usd) >= ?
			AND counted > 0 AND wins >= counted * ?
			AND tokens >= ?
			AND tokens_loss_over <= tokens * ?
			AND wallet NOT IN (
				SELECT wallet FROM wallet_stats WHERE chain_id = ? AND window_days = ? AND version = ? AND `+botCondition+`
			)
		ORDER BY realized_usd DESC, wallet
		LIMIT ?`, args...).Scan(&wallets).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse store: smart money wallets: %w", err)
	}
	return wallets, nil
}

func (repo *statsRepo) SaveRefresh(ctx context.Context, refresh biz.Refresh) error {
	rows := []chdb.WalletRefreshRow{{
		ChainID: refresh.ChainID, Version: refresh.Version, Started: refresh.Started, HeadTime: refresh.Head.UTC(),
		ApplyHeight: refresh.ApplyHeight,
	}}
	if err := chdb.Insert(ctx, repo.db, &rows); err != nil {
		return fmt.Errorf("clickhouse store: save wallet refresh: %w", err)
	}
	return nil
}

// labelRepo 在 PostgreSQL 里保存钱包标签。
type labelRepo struct {
	db *gorm.DB
}

// NewLabelRepo 返回标签仓储。
func NewLabelRepo(data *Data) biz.LabelRepo { return &labelRepo{db: data.state} }

// labelEntry 是写入用的 JSON 形状。
type labelEntry struct {
	Wallet string    `json:"wallet"`
	Label  string    `json:"label"`
	Since  time.Time `json:"since"`
}

func (repo *labelRepo) ReplaceRuleLabels(ctx context.Context, chainID uint64, labels []model.WalletLabel) error {
	entries := make([]labelEntry, len(labels))
	for index, label := range labels {
		entries[index] = labelEntry{Wallet: label.Wallet, Label: label.Label, Since: label.Since.UTC()}
	}
	payload, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("wallet store: encode labels: %w", err)
	}
	values := map[string]any{"chain": int64(chainID), "labels": string(payload), "source": model.WalletLabelSource}
	err = repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := pgdb.Runner{Tx: tx}
		run.Exec(`DELETE FROM wallet_labels l
			WHERE l.chain_id = @chain AND l.source = @source AND NOT EXISTS (
				SELECT 1 FROM jsonb_to_recordset(CAST(@labels AS jsonb)) AS x(wallet text, label text)
				WHERE x.wallet = l.wallet AND x.label = l.label
			)`, values)
		// 已有的标签（包括同名的人工标签）保留原来的来源和生效时间。
		run.Exec(`INSERT INTO wallet_labels (chain_id, wallet, label, source, since)
			SELECT @chain, x.wallet, x.label, @source, x.since
			FROM jsonb_to_recordset(CAST(@labels AS jsonb)) AS x(wallet text, label text, since timestamptz)
			ON CONFLICT DO NOTHING`, values)
		return run.Err
	})
	if err != nil {
		return fmt.Errorf("postgres store: replace wallet labels chain %d: %w", chainID, err)
	}
	return nil
}
