package model

import (
	"math/big"
	"time"

	"github.com/shopspring/decimal"
)

// RawBlock is one block's facts exactly as chainstream-evm published them,
// with addresses lowercased. ch-loader stores it without deriving anything.
type RawBlock struct {
	ChainID   uint64
	Height    int64
	BlockTime time.Time
	Transfers []RawTransfer
	Trades    []RawTrade
	Oracles   []RawOracleUpdate
	Assets    []RawAssetFact
	Pools     []RawPoolFact
}

// Rows counts the facts in the block.
func (block RawBlock) Rows() int {
	return len(block.Transfers) + len(block.Trades) + len(block.Oracles) + len(block.Assets) + len(block.Pools)
}

// RawTransfer is one ERC-20 Transfer log.
type RawTransfer struct {
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	Token            string
	From             string
	To               string
	Amount           *big.Int
}

// RawTrade is one pool operation before base/quote orientation. Amounts are
// pool-perspective signed deltas aligned with the pool's tokens.
type RawTrade struct {
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	OperationIndex   uint32
	Protocol         string
	DexAddress       string
	PoolID           string
	// Action is "swap", "liquidity_added", "liquidity_removed" or
	// "unspecified".
	Action    string
	Sender    string
	Trader    string
	Recipient string
	Amounts   []RawAmount
	// The pool state fields are empty when the protocol event does not
	// expose them.
	ReservesAfter []string
	SqrtPriceX96  string
	Liquidity     string
	Tick          string
	ActiveBinID   *uint32
}

// RawAmount is one token leg of a trade. Raw is a signed integer string;
// Value is decimal-adjusted and empty when the decimals are unknown.
type RawAmount struct {
	Asset         string
	Raw           string
	Value         string
	Decimals      uint32
	DecimalsKnown bool
}

// RawOracleUpdate is one Chainlink-style AnswerUpdated log, from any feed.
type RawOracleUpdate struct {
	TransactionHash  string
	TransactionIndex int64
	LogIndex         uint32
	Feed             string
	Answer           *big.Int
	RoundID          *big.Int
	UpdatedAt        int64
}

// RawAssetFact is a token observation or creation.
type RawAssetFact struct {
	Address          string
	Standard         string
	Recognition      string
	MetadataComplete bool
	// Type is "observed", "created" or "unspecified".
	Type         string
	SourceHeight int64
	// Deployment fields are zero unless the asset was created.
	DeploymentHeight int64
	DeploymentTime   int64
	DeploymentTxIdx  int64
	DeploymentTxHash string
	DeploymentTrace  string
	MetadataJSON     string
}

// RawPoolFact is a pool observation or creation.
type RawPoolFact struct {
	PoolID         string
	Protocol       string
	DexAddress     string
	TokenAddresses []string
	SourceHeight   int64
	SourceTime     int64
	Created        bool
	MetadataJSON   string
}

// RawTradeAt is a stored raw trade with the block that published it.
type RawTradeAt struct {
	Height    int64
	BlockTime time.Time
	RawTrade
}

// PricePoint is a token's stored USD price as of a block.
type PricePoint struct {
	Height int64
	USD    decimal.Decimal
	Hops   uint8
}

// Transfer is a stored ERC-20 transfer as the API returns it.
type Transfer struct {
	BlockHeight int64
	BlockTime   time.Time
	RawTransfer
}

// TransferQuery selects transfers of Token, to or from Holder, or both,
// newest first. Paging continues below (BeforeHeight, BeforeLogIndex) when
// BeforeHeight is positive; BeforeLogIndex nil means the whole height.
type TransferQuery struct {
	ChainID        uint64
	Token          string
	Holder         string
	BeforeHeight   int64
	BeforeLogIndex *uint32
	Limit          int
}
