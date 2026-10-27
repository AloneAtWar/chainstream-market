package model

// Deployment is the transaction that created a contract.
type Deployment struct {
	BlockHeight      int64  `json:"block_height"`
	BlockTime        int64  `json:"block_time"`
	TransactionIndex int64  `json:"transaction_index"`
	TransactionHash  string `json:"transaction_hash"`
	TraceID          string `json:"trace_id"`
}

// RecognitionRecognized is the AssetFact recognition of a standard whose
// probes the address answered.
const RecognitionRecognized = "recognized"

// Token is the latest metadata known for one asset address. Name, Symbol
// and Decimals are nil until the asset's metadata reports them.
type Token struct {
	Address          string
	Standard         string
	Recognition      string
	MetadataComplete bool
	Name             *string
	Symbol           *string
	Decimals         *uint8
	// Metadata is the asset.Info JSON as chainstream-evm published it.
	Metadata []byte
	// Created is set once any fact reports the contract's deployment.
	Created    bool
	Deployment *Deployment
	// SourceHeight is the height the metadata was read at; a fact replaces
	// the stored metadata only when its SourceHeight is not lower, and a
	// fact that does not recognize the address never replaces a row that
	// another standard recognized.
	SourceHeight int64
	// FirstHeight and UpdatedHeight are the blocks that first stored the
	// token and last replaced its metadata. The store sets both.
	FirstHeight   int64
	UpdatedHeight int64
}

// Pool is the latest metadata known for one DEX pool.
type Pool struct {
	PoolID     string
	Protocol   string
	DexAddress string
	Tokens     []string
	Created    bool
	// SourceHeight and SourceTime are when the pool was observed; a fact
	// replaces the stored pool only when its SourceHeight is not lower.
	SourceHeight int64
	SourceTime   int64
	// Metadata is the protocol-specific pool JSON, or nil.
	Metadata      []byte
	FirstHeight   int64
	UpdatedHeight int64
}

// RegistryChange is what one record changed in the registry.
type RegistryChange struct {
	// Reverted undoes every change above RevertTo before anything else.
	Reverted bool
	RevertTo int64
	// Height is the block that Tokens and Pools belong to. Each address
	// appears at most once per standard, and each pool at most once.
	Height int64
	Tokens []Token
	Pools  []Pool
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}
