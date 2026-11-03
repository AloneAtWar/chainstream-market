package pgdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// walletSchema 建 market-wallet 的 PostgreSQL 表。wallet_positions 的列名和 PositionRow 的
// JSON 字段一一对应：写入和回滚都用 jsonb_populate_recordset 按列名还原行。
var walletSchema = []string{
	`CREATE TABLE IF NOT EXISTS wallet_checkpoints (
		chain_id       BIGINT PRIMARY KEY,
		kafka_offset   BIGINT NOT NULL,
		block_height   BIGINT NOT NULL,
		part           INT NOT NULL,
		pruned_through BIGINT NOT NULL,
		updated_at     TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS wallet_positions (
		chain_id         BIGINT NOT NULL,
		wallet           TEXT NOT NULL,
		token            TEXT NOT NULL,
		quantity         NUMERIC NOT NULL,
		cost_usd         NUMERIC NOT NULL,
		realized_usd     NUMERIC NOT NULL,
		bought_qty       NUMERIC NOT NULL,
		sold_qty         NUMERIC NOT NULL,
		bought_usd       NUMERIC NOT NULL,
		sold_usd         NUMERIC NOT NULL,
		buys             BIGINT NOT NULL,
		sells            BIGINT NOT NULL,
		wins             BIGINT NOT NULL,
		losses           BIGINT NOT NULL,
		first_buy_height BIGINT NOT NULL,
		first_buy_time   TIMESTAMPTZ,
		opened_at        TIMESTAMPTZ,
		last_trade_time  TIMESTAMPTZ NOT NULL,
		updated_height   BIGINT NOT NULL,
		PRIMARY KEY (chain_id, wallet, token)
	)`,
	// 代币页的盈利排行按代币读、按已实现盈亏排序。
	`CREATE INDEX IF NOT EXISTS wallet_positions_token ON wallet_positions (chain_id, token, realized_usd DESC)`,
	// 钱包页按最近交易时间列持仓。
	`CREATE INDEX IF NOT EXISTS wallet_positions_recent ON wallet_positions (chain_id, wallet, last_trade_time DESC)`,
	// 每行是一个持仓在 block_height 第一次被改动之前的样子；prev 为 NULL 表示该高度新建了它。
	`CREATE TABLE IF NOT EXISTS wallet_undo (
		chain_id     BIGINT NOT NULL,
		block_height BIGINT NOT NULL,
		wallet       TEXT NOT NULL,
		token        TEXT NOT NULL,
		prev         JSONB,
		PRIMARY KEY (chain_id, block_height, wallet, token)
	)`,
	`CREATE TABLE IF NOT EXISTS wallet_labels (
		chain_id BIGINT NOT NULL,
		wallet   TEXT NOT NULL,
		label    TEXT NOT NULL,
		source   TEXT NOT NULL,
		since    TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (chain_id, wallet, label)
	)`,
	`CREATE INDEX IF NOT EXISTS wallet_labels_label ON wallet_labels (chain_id, label)`,
}

// PositionRow 是 wallet_positions 的一行，也是写入和 undo 用的 JSON 形状：数值是十进制
// 字符串，时间是 RFC 3339，没有的时间为 null。
type PositionRow struct {
	ChainID        int64      `json:"chain_id"`
	Wallet         string     `json:"wallet"`
	Token          string     `json:"token"`
	Quantity       string     `json:"quantity"`
	CostUSD        string     `json:"cost_usd" gorm:"column:cost_usd"`
	RealizedUSD    string     `json:"realized_usd" gorm:"column:realized_usd"`
	BoughtQty      string     `json:"bought_qty"`
	SoldQty        string     `json:"sold_qty"`
	BoughtUSD      string     `json:"bought_usd" gorm:"column:bought_usd"`
	SoldUSD        string     `json:"sold_usd" gorm:"column:sold_usd"`
	Buys           int64      `json:"buys"`
	Sells          int64      `json:"sells"`
	Wins           int64      `json:"wins"`
	Losses         int64      `json:"losses"`
	FirstBuyHeight int64      `json:"first_buy_height"`
	FirstBuyTime   *time.Time `json:"first_buy_time"`
	OpenedAt       *time.Time `json:"opened_at"`
	LastTradeTime  time.Time  `json:"last_trade_time"`
	UpdatedHeight  int64      `json:"updated_height"`
}

// NewPositionRow 把持仓转成行。
func NewPositionRow(chainID uint64, position model.Position) PositionRow {
	return PositionRow{
		ChainID: int64(chainID), Wallet: position.Wallet, Token: position.Token,
		Quantity: position.Quantity.String(), CostUSD: position.CostUSD.String(), RealizedUSD: position.RealizedUSD.String(),
		BoughtQty: position.BoughtQty.String(), SoldQty: position.SoldQty.String(),
		BoughtUSD: position.BoughtUSD.String(), SoldUSD: position.SoldUSD.String(),
		Buys: position.Buys, Sells: position.Sells, Wins: position.Wins, Losses: position.Losses,
		FirstBuyHeight: position.FirstBuyHeight, FirstBuyTime: optionalTime(position.FirstBuyTime),
		OpenedAt: optionalTime(position.OpenedAt), LastTradeTime: position.LastTradeTime.UTC(),
		UpdatedHeight: position.UpdatedHeight,
	}
}

// Position 把行转回持仓。
func (row PositionRow) Position() (model.Position, error) {
	position := model.Position{
		Wallet: row.Wallet, Token: row.Token, Buys: row.Buys, Sells: row.Sells, Wins: row.Wins, Losses: row.Losses,
		FirstBuyHeight: row.FirstBuyHeight, LastTradeTime: row.LastTradeTime.UTC(), UpdatedHeight: row.UpdatedHeight,
	}
	if row.FirstBuyTime != nil {
		position.FirstBuyTime = row.FirstBuyTime.UTC()
	}
	if row.OpenedAt != nil {
		position.OpenedAt = row.OpenedAt.UTC()
	}
	for _, field := range []struct {
		target *decimal.Decimal
		value  string
	}{
		{&position.Quantity, row.Quantity}, {&position.CostUSD, row.CostUSD}, {&position.RealizedUSD, row.RealizedUSD},
		{&position.BoughtQty, row.BoughtQty}, {&position.SoldQty, row.SoldQty},
		{&position.BoughtUSD, row.BoughtUSD}, {&position.SoldUSD, row.SoldUSD},
	} {
		value, err := decimal.NewFromString(field.value)
		if err != nil {
			return model.Position{}, fmt.Errorf("postgres store: read position %s/%s: %w", row.Wallet, row.Token, err)
		}
		*field.target = value
	}
	return position, nil
}

// PositionColumns 是按文本读出 wallet_positions 数值列的 SELECT 列表，配合 PositionRow 使用。
const PositionColumns = `p.chain_id, p.wallet, p.token, p.quantity::text AS quantity, p.cost_usd::text AS cost_usd,
	p.realized_usd::text AS realized_usd, p.bought_qty::text AS bought_qty, p.sold_qty::text AS sold_qty,
	p.bought_usd::text AS bought_usd, p.sold_usd::text AS sold_usd, p.buys, p.sells, p.wins, p.losses,
	p.first_buy_height, p.first_buy_time, p.opened_at, p.last_trade_time, p.updated_height`

// Positions 解码多行持仓。
func Positions(rows []PositionRow) ([]model.Position, error) {
	positions := make([]model.Position, len(rows))
	for index, row := range rows {
		position, err := row.Position()
		if err != nil {
			return nil, err
		}
		positions[index] = position
	}
	return positions, nil
}

// MarshalPositions 把持仓编码成 jsonb_populate_recordset(NULL::wallet_positions, ...) 读的 JSON。
func MarshalPositions(chainID uint64, positions []model.Position) (string, error) {
	rows := make([]PositionRow, len(positions))
	for index, position := range positions {
		rows[index] = NewPositionRow(chainID, position)
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return "", fmt.Errorf("postgres store: encode positions: %w", err)
	}
	return string(payload), nil
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	return &utc
}

// WalletLabelRow 是 wallet_labels 的一行。
type WalletLabelRow struct {
	ChainID int64 `gorm:"primaryKey;autoIncrement:false"`
	Wallet  string
	Label   string
	Source  string
	Since   time.Time
}

// TableName 返回表名。
func (WalletLabelRow) TableName() string { return "wallet_labels" }

// WalletLabels 返回 wallets 中各钱包的标签，按钱包分组；wallets 为空时返回 nil。
func WalletLabels(ctx context.Context, db *gorm.DB, chainID uint64, wallets []string) (map[string][]string, error) {
	if len(wallets) == 0 {
		return nil, nil
	}
	var rows []WalletLabelRow
	err := db.WithContext(ctx).Where("chain_id = ? AND wallet = ANY(?)", int64(chainID), Array(wallets)).
		Order("wallet, label").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: wallet labels: %w", err)
	}
	labels := make(map[string][]string)
	for _, row := range rows {
		labels[row.Wallet] = append(labels[row.Wallet], row.Label)
	}
	return labels, nil
}

// LabeledWallets 返回链上带 label 标签的全部钱包。
func LabeledWallets(ctx context.Context, db *gorm.DB, chainID uint64, label string) ([]string, error) {
	var wallets []string
	err := db.WithContext(ctx).Model(&WalletLabelRow{}).Where("chain_id = ? AND label = ?", int64(chainID), label).
		Order("wallet").Pluck("wallet", &wallets).Error
	if err != nil {
		return nil, fmt.Errorf("postgres store: labeled wallets: %w", err)
	}
	return wallets, nil
}

// AllWalletLabels 返回链上全部钱包标签，按钱包分组，供 market-api 的推送频道过滤。
func AllWalletLabels(ctx context.Context, db *gorm.DB, chainID uint64) (map[string][]string, error) {
	var rows []WalletLabelRow
	if err := db.WithContext(ctx).Where("chain_id = ?", int64(chainID)).Order("wallet, label").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("postgres store: all wallet labels: %w", err)
	}
	labels := make(map[string][]string)
	for _, row := range rows {
		labels[row.Wallet] = append(labels[row.Wallet], row.Label)
	}
	return labels, nil
}
