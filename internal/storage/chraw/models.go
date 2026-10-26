package chraw

import (
	"math/big"
	"time"
)

// The models below are the rows ch-loader writes, one per table. Their
// fields follow the table columns.

type transferRow struct {
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

func (transferRow) TableName() string { return "raw_transfers" }

type tradeRow struct {
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

func (tradeRow) TableName() string { return "raw_trades" }

type oracleRow struct {
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

func (oracleRow) TableName() string { return "oracle_updates" }

type assetRow struct {
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

func (assetRow) TableName() string { return "asset_facts" }

type poolRow struct {
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

func (poolRow) TableName() string { return "pool_facts" }

type checkpointRow struct {
	ChainID     uint64
	KafkaOffset int64
	BlockHeight int64
	BlockHash   string
	UpdatedAt   time.Time `gorm:"autoUpdateTime:false"`
}

func (checkpointRow) TableName() string { return "raw_checkpoints" }
