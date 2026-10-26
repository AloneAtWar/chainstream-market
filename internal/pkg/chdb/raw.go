package chdb

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// RawTables lists every raw fact table; each is deleted by block_height.
var RawTables = []string{"raw_transfers", "raw_trades", "oracle_updates", "asset_facts", "pool_facts"}

// LoadRawCheckpoint returns ch-loader's current checkpoint of chainID.
func LoadRawCheckpoint(ctx context.Context, db *gorm.DB, chainID uint64) (chainsync.Checkpoint, bool, error) {
	var rows []RawCheckpointRow
	err := db.WithContext(ctx).Select("kafka_offset, block_height, block_hash, updated_at").
		Where("chain_id = ?", chainID).Order("kafka_offset DESC").Limit(1).Find(&rows).Error
	if err != nil {
		return chainsync.Checkpoint{}, false, fmt.Errorf("clickhouse raw store: load checkpoint: %w", err)
	}
	if len(rows) == 0 {
		return chainsync.Checkpoint{}, false, nil
	}
	row := rows[0]
	return chainsync.Checkpoint{
		ChainID: chainID, Offset: row.KafkaOffset, Height: row.BlockHeight, Hash: row.BlockHash,
		UpdatedAt: row.UpdatedAt.UTC(),
	}, true, nil
}

// SaveRawCheckpoint adds checkpoint as ch-loader's current one.
func SaveRawCheckpoint(ctx context.Context, db *gorm.DB, checkpoint chainsync.Checkpoint) error {
	if checkpoint.ChainID == 0 {
		return errors.New("clickhouse raw store: checkpoint chain id is required")
	}
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	return insertRaw(ctx, db, "raw_checkpoints", &[]RawCheckpointRow{{
		ChainID: checkpoint.ChainID, KafkaOffset: checkpoint.Offset, BlockHeight: checkpoint.Height,
		BlockHash: checkpoint.Hash, UpdatedAt: updatedAt.UTC(),
	}})
}

// WriteRaw inserts the facts of blocks, which must not already be stored,
// with one insert per table, skipping tables with no rows.
func WriteRaw(ctx context.Context, db *gorm.DB, blocks []model.RawBlock) error {
	for _, block := range blocks {
		if block.ChainID == 0 {
			return errors.New("clickhouse raw store: block chain id is required")
		}
	}
	var (
		transfers []TransferRow
		trades    []TradeRow
		oracles   []OracleRow
		assets    []AssetRow
		pools     []PoolRow
	)
	for _, block := range blocks {
		transfers = appendTransfers(transfers, block)
		trades = appendTrades(trades, block)
		oracles = appendOracles(oracles, block)
		assets = appendAssets(assets, block)
		pools = appendPools(pools, block)
	}
	writes := []struct {
		table string
		count int
		rows  any
	}{
		{"raw_transfers", len(transfers), &transfers},
		{"raw_trades", len(trades), &trades},
		{"oracle_updates", len(oracles), &oracles},
		{"asset_facts", len(assets), &assets},
		{"pool_facts", len(pools), &pools},
	}
	for _, write := range writes {
		if write.count == 0 {
			continue
		}
		if err := insertRaw(ctx, db, write.table, write.rows); err != nil {
			return err
		}
	}
	return nil
}

func appendTransfers(rows []TransferRow, block model.RawBlock) []TransferRow {
	for _, transfer := range block.Transfers {
		rows = append(rows, TransferRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: transfer.TransactionHash, TransactionIndex: transfer.TransactionIndex,
			LogIndex: transfer.LogIndex, Token: transfer.Token, FromAddress: transfer.From, ToAddress: transfer.To,
			Amount: transfer.Amount,
		})
	}
	return rows
}

func appendTrades(rows []TradeRow, block model.RawBlock) []TradeRow {
	for _, trade := range block.Trades {
		row := TradeRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: trade.TransactionHash, TransactionIndex: trade.TransactionIndex,
			LogIndex: trade.LogIndex, OperationIndex: trade.OperationIndex, Protocol: trade.Protocol,
			DexAddress: trade.DexAddress, PoolID: trade.PoolID, Action: trade.Action, Sender: trade.Sender,
			Trader: trade.Trader, Recipient: trade.Recipient,
			AmountAssets: make([]string, len(trade.Amounts)), AmountRaw: make([]string, len(trade.Amounts)),
			AmountValues: make([]string, len(trade.Amounts)), AmountDecimals: make([]uint32, len(trade.Amounts)),
			AmountDecimalsKnown: make([]bool, len(trade.Amounts)), ReservesAfter: trade.ReservesAfter,
			SqrtPriceX96: trade.SqrtPriceX96, Liquidity: trade.Liquidity, Tick: trade.Tick, ActiveBinID: trade.ActiveBinID,
		}
		for index, amount := range trade.Amounts {
			row.AmountAssets[index], row.AmountRaw[index], row.AmountValues[index] = amount.Asset, amount.Raw, amount.Value
			row.AmountDecimals[index], row.AmountDecimalsKnown[index] = amount.Decimals, amount.DecimalsKnown
		}
		if row.ReservesAfter == nil {
			row.ReservesAfter = []string{}
		}
		rows = append(rows, row)
	}
	return rows
}

func appendOracles(rows []OracleRow, block model.RawBlock) []OracleRow {
	for _, update := range block.Oracles {
		rows = append(rows, OracleRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime,
			TransactionHash: update.TransactionHash, TransactionIndex: update.TransactionIndex,
			LogIndex: update.LogIndex, Feed: update.Feed, Answer: update.Answer, RoundID: update.RoundID,
			UpdatedAt: update.UpdatedAt,
		})
	}
	return rows
}

func appendAssets(rows []AssetRow, block model.RawBlock) []AssetRow {
	for _, fact := range block.Assets {
		rows = append(rows, AssetRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime, Address: fact.Address,
			Standard: fact.Standard, Recognition: fact.Recognition, MetadataComplete: fact.MetadataComplete,
			Type: fact.Type, SourceHeight: fact.SourceHeight, DeploymentHeight: fact.DeploymentHeight,
			DeploymentTime: fact.DeploymentTime, DeploymentTxIndex: fact.DeploymentTxIdx,
			DeploymentTxHash: fact.DeploymentTxHash, DeploymentTraceID: fact.DeploymentTrace,
			MetadataJSON: fact.MetadataJSON,
		})
	}
	return rows
}

func appendPools(rows []PoolRow, block model.RawBlock) []PoolRow {
	for _, fact := range block.Pools {
		tokens := fact.TokenAddresses
		if tokens == nil {
			tokens = []string{}
		}
		rows = append(rows, PoolRow{
			ChainID: block.ChainID, BlockHeight: block.Height, BlockTime: block.BlockTime, PoolID: fact.PoolID,
			Protocol: fact.Protocol, DexAddress: fact.DexAddress, TokenAddresses: tokens,
			SourceHeight: fact.SourceHeight, SourceTime: fact.SourceTime, Created: fact.Created,
			MetadataJSON: fact.MetadataJSON,
		})
	}
	return rows
}

func insertRaw(ctx context.Context, db *gorm.DB, table string, rows any) error {
	if err := Insert(ctx, db, rows); err != nil {
		return fmt.Errorf("clickhouse raw store: insert %s: %w", table, err)
	}
	return nil
}

// The models below are the rows ch-loader writes, one per table of
// RawSchema. Their fields follow the table columns. market-engine's backfills
// read the trades and market-api the transfers.

// TransferRow is a raw_transfers row.
type TransferRow struct {
	ChainID          uint64
	BlockHeight      int64
	BlockTime        time.Time
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	Token            string
	FromAddress      string
	ToAddress        string
	Amount           *big.Int `gorm:"type:UInt256"`
}

func (TransferRow) TableName() string { return "raw_transfers" }

// TradeRow is a raw_trades row: one unoriented trade.
type TradeRow struct {
	ChainID             uint64
	BlockHeight         int64
	BlockTime           time.Time
	TransactionHash     string
	TransactionIndex    int64
	LogIndex            uint32
	OperationIndex      uint32
	Protocol            string
	DexAddress          string
	PoolID              string
	Action              string
	Sender              string
	Trader              string
	Recipient           string
	AmountAssets        []string `gorm:"type:Array(String)"`
	AmountRaw           []string `gorm:"type:Array(String)"`
	AmountValues        []string `gorm:"type:Array(String)"`
	AmountDecimals      []uint32 `gorm:"type:Array(UInt32)"`
	AmountDecimalsKnown []bool   `gorm:"type:Array(Bool)"`
	ReservesAfter       []string `gorm:"type:Array(String)"`
	SqrtPriceX96        string   `gorm:"column:sqrt_price_x96"`
	Liquidity           string
	Tick                string
	ActiveBinID         *uint32
}

func (TradeRow) TableName() string { return "raw_trades" }

// OracleRow is an oracle_updates row.
type OracleRow struct {
	ChainID          uint64
	BlockHeight      int64
	BlockTime        time.Time
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	Feed             string
	Answer           *big.Int `gorm:"type:Int256"`
	RoundID          *big.Int `gorm:"type:UInt256"`
	UpdatedAt        int64    `gorm:"autoUpdateTime:false"`
}

func (OracleRow) TableName() string { return "oracle_updates" }

// AssetRow is an asset_facts row.
type AssetRow struct {
	ChainID           uint64
	BlockHeight       int64
	BlockTime         time.Time
	Address           string
	Standard          string
	Recognition       string
	MetadataComplete  bool
	Type              string
	SourceHeight      int64
	DeploymentHeight  int64
	DeploymentTime    int64
	DeploymentTxIndex int64
	DeploymentTxHash  string
	DeploymentTraceID string `gorm:"column:deployment_trace_id"`
	MetadataJSON      string `gorm:"column:metadata_json"`
}

func (AssetRow) TableName() string { return "asset_facts" }

// PoolRow is a pool_facts row.
type PoolRow struct {
	ChainID        uint64
	BlockHeight    int64
	BlockTime      time.Time
	PoolID         string
	Protocol       string
	DexAddress     string
	TokenAddresses []string `gorm:"type:Array(String)"`
	SourceHeight   int64
	SourceTime     int64
	Created        bool
	MetadataJSON   string `gorm:"column:metadata_json"`
}

func (PoolRow) TableName() string { return "pool_facts" }

// RawCheckpointRow is a raw_checkpoints row. The table keeps every
// checkpoint; the one with the highest offset is current.
type RawCheckpointRow struct {
	ChainID     uint64
	KafkaOffset int64
	BlockHeight int64
	BlockHash   string
	UpdatedAt   time.Time `gorm:"autoUpdateTime:false"`
}

func (RawCheckpointRow) TableName() string { return "raw_checkpoints" }
