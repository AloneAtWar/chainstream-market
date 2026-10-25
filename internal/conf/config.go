// Package conf loads the market-engine YAML configuration.
package conf

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the market-engine process configuration.
type Config struct {
	Kafka    Kafka    `yaml:"kafka"`
	Storage  Storage  `yaml:"storage"`
	State    State    `yaml:"state"`
	Market   Market   `yaml:"market"`
	API      API      `yaml:"api"`
	Holders  Holders  `yaml:"holders"`
	Registry Registry `yaml:"registry"`
	Rankings Rankings `yaml:"rankings"`
	// ChLoader is named after the ch-loader command.
	ChLoader ChLoader `yaml:"ch_loader"`
	Live     Live     `yaml:"live"`
	Chains   []Chain  `yaml:"chains"`
}

// Live configures realtime push. market-engine publishes each block's trades
// and candles, and reorg corrections, to every chain's live_topic, and
// market-api streams them to WebSocket clients at /v1/stream.
type Live struct {
	Enabled bool `yaml:"enabled"`
	// MaxLag skips blocks further than this behind the clock, so a catch-up
	// does not push history; a negative value such as -1s pushes every block.
	// Reverts always go out.
	MaxLag time.Duration `yaml:"max_lag"`
	// CandleThrottle is how often market-api sends each changed candle
	// bucket, at its latest value. Trades are sent at once.
	CandleThrottle time.Duration `yaml:"candle_throttle"`
	// ClientBuffer is how many messages may wait for one client before it
	// is disconnected as too slow.
	ClientBuffer     int `yaml:"client_buffer"`
	MaxSubscriptions int `yaml:"max_subscriptions"`
	// AllowedOrigins lists other origins browsers may connect from, as host
	// patterns such as "localhost:5173". Same-origin requests and clients
	// that send no Origin are always allowed.
	AllowedOrigins []string `yaml:"allowed_origins"`
}

// ChLoader tunes ch-loader, which copies raw chain facts into the
// storage.clickhouse database. Blocks older than BatchLag are written in
// batches of up to FlushRows facts or FlushInterval; newer blocks are written
// one by one.
type ChLoader struct {
	BatchLag      time.Duration `yaml:"batch_lag"`
	FlushRows     int           `yaml:"flush_rows"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// Rankings tunes market-rankings, which rolls market-engine's ClickHouse
// output, holder counts and registry metadata into the token_rankings table
// that market-api lists.
type Rankings struct {
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	// CandleInterval is the token candle size summed for the 24h columns;
	// it must be one of market.intervals.
	CandleInterval time.Duration `yaml:"candle_interval"`
	// ReorgWindow is how far behind the last refresh's chain head latest
	// prices are read again, so a reorg that moved them is picked up.
	ReorgWindow time.Duration `yaml:"reorg_window"`
}

// Registry tunes market-registry, which keeps token and pool metadata in
// the state.postgres database.
type Registry struct {
	// HistoryDepth is how many recent blocks stay revertible in addition to
	// everything above the finalized height.
	HistoryDepth int64 `yaml:"history_depth"`
}

// Holders tunes holder-indexer, which keeps ERC-20 balances per holder in
// the state.postgres database.
type Holders struct {
	// HistoryDepth is how many recent blocks stay revertible in addition to
	// everything above the finalized height.
	HistoryDepth int64 `yaml:"history_depth"`
	// ReconcileEvery is how often, in blocks, the least recently verified
	// holdings are checked against balanceOf. Negative disables it.
	ReconcileEvery int64 `yaml:"reconcile_every"`
	ReconcileBatch int   `yaml:"reconcile_batch"`
	// RPCBatchSize caps the balanceOf calls in one JSON-RPC batch.
	RPCBatchSize int           `yaml:"rpc_batch_size"`
	RPCTimeout   time.Duration `yaml:"rpc_timeout"`
}

// API configures market-api, which reads the ClickHouse store.
type API struct {
	Addr string `yaml:"addr"`
}

// Market tunes pricing and candle aggregation for every chain.
type Market struct {
	// MinLiquidityUSD is the counterpart reserve value a pool needs before a
	// price propagates through it.
	MinLiquidityUSD float64 `yaml:"min_liquidity_usd"`
	// CandleMinLiquidityUSD is the quote reserve value a pool needs before
	// its trades join a token's aggregated candles.
	CandleMinLiquidityUSD float64         `yaml:"candle_min_liquidity_usd"`
	MaxHops               uint8           `yaml:"max_hops"`
	Intervals             []time.Duration `yaml:"intervals"`
	// HistoryDepth is how many recent blocks stay revertible in addition to
	// everything above the finalized height.
	HistoryDepth int64 `yaml:"history_depth"`
	// OracleDeviation is the largest relative gap between a feed-backed
	// token's graph price and its oracle answer before the answer wins.
	OracleDeviation float64 `yaml:"oracle_deviation"`
	// LateTradeBlocks is how many blocks a trade whose token decimals are
	// unknown waits for them before it is dropped; -1 drops it at once.
	LateTradeBlocks int64 `yaml:"late_trade_blocks"`
	// LateTradesPerToken caps the trades held while waiting on one token.
	LateTradesPerToken int `yaml:"late_trades_per_token"`
}

// Kafka configures the per-chain topic consumers.
type Kafka struct {
	Brokers  []string `yaml:"brokers"`
	ClientID string   `yaml:"client_id"`
	// FetchMaxBytes must be at least chainstream-evm's max_message_bytes, or a
	// large block can never be fetched.
	FetchMaxBytes int32         `yaml:"fetch_max_bytes"`
	FetchMaxWait  time.Duration `yaml:"fetch_max_wait"`
}

// Storage selects where market output goes.
type Storage struct {
	// Driver is "memory" or "clickhouse".
	Driver     string     `yaml:"driver"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
}

// State selects where checkpoints and processor state are kept. In memory, a
// restart replays each topic from offset zero and the output store
// overwrites the heights it already holds. In PostgreSQL, every record's
// state changes are committed with its checkpoint and a restart resumes at
// the next record.
type State struct {
	// Driver is "memory" or "postgres".
	Driver   string   `yaml:"driver"`
	Postgres Postgres `yaml:"postgres"`
}

type Postgres struct {
	DSN string `yaml:"dsn"`
	// Schema holds the tables; empty uses public.
	Schema string `yaml:"schema"`
}

type ClickHouse struct {
	Addr        []string      `yaml:"addr"`
	Database    string        `yaml:"database"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

// Chain is one chainstream-evm topic consumed in order by its own engine.
type Chain struct {
	Name    string `yaml:"name"`
	ChainID uint64 `yaml:"chain_id"`
	Topic   string `yaml:"topic"`
	// RPCURL is a JSON-RPC endpoint holder-indexer reads balanceOf from. It
	// must serve state by block hash for blocks the indexer is applying,
	// which near the head any full node does.
	RPCURL string `yaml:"rpc_url"`
	// LiveTopic carries this chain's live updates when live.enabled is set;
	// it defaults to the topic name with a "-live" suffix.
	LiveTopic string `yaml:"live_topic"`
	// Feeds are the Chainlink aggregators trusted on this chain. Use the
	// aggregator behind each proxy, since it is the contract that emits
	// AnswerUpdated.
	Feeds []Feed `yaml:"feeds"`
}

// Feed maps a USD-denominated oracle aggregator to the token it prices.
type Feed struct {
	Address  string `yaml:"address"`
	Token    string `yaml:"token"`
	Decimals uint8  `yaml:"decimals"`
}

const (
	StorageMemory     = "memory"
	StorageClickHouse = "clickhouse"

	StateMemory   = "memory"
	StatePostgres = "postgres"

	defaultClickHouseDatabase = "market"
	defaultAPIAddr            = ":8080"

	defaultFetchMaxBytes = 104857600
	defaultFetchMaxWait  = 500 * time.Millisecond

	defaultMinLiquidityUSD       = 1000
	defaultCandleMinLiquidityUSD = 10000
	defaultMaxHops               = 3
	defaultHistoryDepth          = 256
	defaultOracleDeviation       = 0.02
	defaultLateTradeBlocks       = 7200
	defaultLateTradesPerToken    = 1000

	defaultHoldersReconcileEvery = 100
	defaultHoldersReconcileBatch = 200
	defaultRPCBatchSize          = 100
	defaultRPCTimeout            = 30 * time.Second

	defaultRankingsRefreshInterval = time.Minute
	defaultRankingsCandleInterval  = 5 * time.Minute
	defaultRankingsReorgWindow     = time.Hour

	defaultLoaderBatchLag      = 5 * time.Minute
	defaultLoaderFlushRows     = 100000
	defaultLoaderFlushInterval = 2 * time.Second

	defaultLiveMaxLag           = 10 * time.Minute
	defaultLiveCandleThrottle   = 250 * time.Millisecond
	defaultLiveClientBuffer     = 1024
	defaultLiveMaxSubscriptions = 200
	liveTopicSuffix             = "-live"
)

// DefaultIntervals are the candle sizes used when none are configured.
func DefaultIntervals() []time.Duration {
	return []time.Duration{
		time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour,
	}
}

// Load reads, defaults and validates the configuration file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("conf: read %s: %w", path, err)
	}
	return Parse(raw)
}

// Parse decodes, defaults and validates raw YAML.
func Parse(raw []byte) (*Config, error) {
	var config Config
	if err := yaml.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("conf: decode: %w", err)
	}
	config.applyDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (config *Config) applyDefaults() {
	if config.Kafka.FetchMaxBytes == 0 {
		config.Kafka.FetchMaxBytes = defaultFetchMaxBytes
	}
	if config.Kafka.FetchMaxWait == 0 {
		config.Kafka.FetchMaxWait = defaultFetchMaxWait
	}
	if config.Market.MinLiquidityUSD == 0 {
		config.Market.MinLiquidityUSD = defaultMinLiquidityUSD
	}
	if config.Market.CandleMinLiquidityUSD == 0 {
		config.Market.CandleMinLiquidityUSD = defaultCandleMinLiquidityUSD
	}
	if config.Market.MaxHops == 0 {
		config.Market.MaxHops = defaultMaxHops
	}
	if len(config.Market.Intervals) == 0 {
		config.Market.Intervals = DefaultIntervals()
	}
	if config.Market.HistoryDepth == 0 {
		config.Market.HistoryDepth = defaultHistoryDepth
	}
	if config.Market.OracleDeviation == 0 {
		config.Market.OracleDeviation = defaultOracleDeviation
	}
	if config.Market.LateTradeBlocks == 0 {
		config.Market.LateTradeBlocks = defaultLateTradeBlocks
	}
	if config.Market.LateTradesPerToken == 0 {
		config.Market.LateTradesPerToken = defaultLateTradesPerToken
	}
	if strings.TrimSpace(config.Storage.Driver) == "" {
		config.Storage.Driver = StorageMemory
	}
	if config.Holders.HistoryDepth == 0 {
		config.Holders.HistoryDepth = defaultHistoryDepth
	}
	if config.Registry.HistoryDepth == 0 {
		config.Registry.HistoryDepth = defaultHistoryDepth
	}
	if config.Holders.ReconcileEvery == 0 {
		config.Holders.ReconcileEvery = defaultHoldersReconcileEvery
	}
	if config.Holders.ReconcileBatch == 0 {
		config.Holders.ReconcileBatch = defaultHoldersReconcileBatch
	}
	if config.Holders.RPCBatchSize == 0 {
		config.Holders.RPCBatchSize = defaultRPCBatchSize
	}
	if config.Holders.RPCTimeout == 0 {
		config.Holders.RPCTimeout = defaultRPCTimeout
	}
	if config.Rankings.RefreshInterval == 0 {
		config.Rankings.RefreshInterval = defaultRankingsRefreshInterval
	}
	if config.Rankings.CandleInterval == 0 {
		config.Rankings.CandleInterval = slices.Min(config.Market.Intervals)
		if slices.Contains(config.Market.Intervals, defaultRankingsCandleInterval) {
			config.Rankings.CandleInterval = defaultRankingsCandleInterval
		}
	}
	if config.Rankings.ReorgWindow == 0 {
		config.Rankings.ReorgWindow = defaultRankingsReorgWindow
	}
	if config.ChLoader.BatchLag == 0 {
		config.ChLoader.BatchLag = defaultLoaderBatchLag
	}
	if config.ChLoader.FlushRows == 0 {
		config.ChLoader.FlushRows = defaultLoaderFlushRows
	}
	if config.ChLoader.FlushInterval == 0 {
		config.ChLoader.FlushInterval = defaultLoaderFlushInterval
	}
	if config.Live.MaxLag == 0 {
		config.Live.MaxLag = defaultLiveMaxLag
	}
	if config.Live.CandleThrottle == 0 {
		config.Live.CandleThrottle = defaultLiveCandleThrottle
	}
	if config.Live.ClientBuffer == 0 {
		config.Live.ClientBuffer = defaultLiveClientBuffer
	}
	if config.Live.MaxSubscriptions == 0 {
		config.Live.MaxSubscriptions = defaultLiveMaxSubscriptions
	}
	for index := range config.Chains {
		if strings.TrimSpace(config.Chains[index].LiveTopic) == "" && config.Chains[index].Topic != "" {
			config.Chains[index].LiveTopic = config.Chains[index].Topic + liveTopicSuffix
		}
	}
	if strings.TrimSpace(config.State.Driver) == "" {
		config.State.Driver = StateMemory
	}
	if strings.TrimSpace(config.API.Addr) == "" {
		config.API.Addr = defaultAPIAddr
	}
	if config.Storage.Driver == StorageClickHouse && config.Storage.ClickHouse.Database == "" {
		config.Storage.ClickHouse.Database = defaultClickHouseDatabase
	}
}

// Validate reports the first invalid setting.
func (config *Config) Validate() error {
	if len(config.Kafka.Brokers) == 0 {
		return errors.New("conf: kafka.brokers is required")
	}
	for _, broker := range config.Kafka.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("conf: kafka.brokers must not contain empty values")
		}
	}
	if config.Kafka.FetchMaxBytes < 0 || config.Kafka.FetchMaxWait < 0 {
		return errors.New("conf: kafka fetch limits must be positive")
	}
	if config.Market.MinLiquidityUSD < 0 || config.Market.CandleMinLiquidityUSD < 0 {
		return errors.New("conf: market liquidity thresholds must not be negative")
	}
	if config.Market.OracleDeviation < 0 {
		return errors.New("conf: market.oracle_deviation must not be negative")
	}
	if config.Market.HistoryDepth < 0 {
		return errors.New("conf: market.history_depth must be positive")
	}
	if config.Market.LateTradeBlocks < -1 || config.Market.LateTradesPerToken < 0 {
		return errors.New("conf: market.late_trade_blocks must be positive or -1, and late_trades_per_token positive")
	}
	for _, interval := range config.Market.Intervals {
		if interval < time.Second || interval%time.Second != 0 {
			return fmt.Errorf("conf: market interval %s must be whole seconds", interval)
		}
	}
	switch config.Storage.Driver {
	case StorageMemory:
	case StorageClickHouse:
		if len(config.Storage.ClickHouse.Addr) == 0 {
			return errors.New("conf: storage.clickhouse.addr is required")
		}
	default:
		return fmt.Errorf("conf: unsupported storage.driver %q", config.Storage.Driver)
	}
	if config.Holders.HistoryDepth < 0 || config.Holders.ReconcileBatch < 0 ||
		config.Holders.RPCBatchSize < 0 || config.Holders.RPCTimeout < 0 {
		return errors.New("conf: holders settings must be positive")
	}
	if config.Registry.HistoryDepth < 0 {
		return errors.New("conf: registry.history_depth must be positive")
	}
	if config.Rankings.RefreshInterval < 0 || config.Rankings.ReorgWindow < 0 {
		return errors.New("conf: rankings durations must be positive")
	}
	if !slices.Contains(config.Market.Intervals, config.Rankings.CandleInterval) || config.Rankings.CandleInterval > 24*time.Hour {
		return fmt.Errorf("conf: rankings.candle_interval %s must be one of market.intervals and at most 24h", config.Rankings.CandleInterval)
	}
	if config.ChLoader.BatchLag < 0 || config.ChLoader.FlushRows < 0 || config.ChLoader.FlushInterval < 0 {
		return errors.New("conf: ch_loader settings must be positive")
	}
	if config.Live.CandleThrottle < 0 || config.Live.ClientBuffer < 0 || config.Live.MaxSubscriptions < 0 {
		return errors.New("conf: live settings must be positive")
	}
	switch config.State.Driver {
	case StateMemory:
	case StatePostgres:
		if strings.TrimSpace(config.State.Postgres.DSN) == "" {
			return errors.New("conf: state.postgres.dsn is required")
		}
	default:
		return fmt.Errorf("conf: unsupported state.driver %q", config.State.Driver)
	}
	if len(config.Chains) == 0 {
		return errors.New("conf: at least one chain is required")
	}
	names := make(map[string]struct{}, len(config.Chains))
	ids := make(map[uint64]struct{}, len(config.Chains))
	topics := make(map[string]struct{}, len(config.Chains))
	for index, chain := range config.Chains {
		if strings.TrimSpace(chain.Name) == "" || chain.ChainID == 0 || strings.TrimSpace(chain.Topic) == "" {
			return fmt.Errorf("conf: chains[%d] needs name, chain_id and topic", index)
		}
		if _, ok := names[chain.Name]; ok {
			return fmt.Errorf("conf: duplicate chain name %q", chain.Name)
		}
		if _, ok := ids[chain.ChainID]; ok {
			return fmt.Errorf("conf: duplicate chain_id %d", chain.ChainID)
		}
		if _, ok := topics[chain.Topic]; ok {
			return fmt.Errorf("conf: duplicate topic %q", chain.Topic)
		}
		names[chain.Name], ids[chain.ChainID], topics[chain.Topic] = struct{}{}, struct{}{}, struct{}{}
		feeds := make(map[string]struct{}, len(chain.Feeds))
		for feedIndex, feed := range chain.Feeds {
			address := strings.ToLower(strings.TrimSpace(feed.Address))
			if address == "" || strings.TrimSpace(feed.Token) == "" || feed.Decimals > 36 {
				return fmt.Errorf("conf: chains[%d].feeds[%d] needs address, token and decimals <= 36", index, feedIndex)
			}
			if _, ok := feeds[address]; ok {
				return fmt.Errorf("conf: chain %s has duplicate feed %s", chain.Name, address)
			}
			feeds[address] = struct{}{}
		}
	}
	return config.validateLiveTopics()
}

// validateLiveTopics checks that no live topic collides with another chain's
// live topic or with any chain topic.
func (config *Config) validateLiveTopics() error {
	if !config.Live.Enabled {
		return nil
	}
	used := make(map[string]struct{}, 2*len(config.Chains))
	for _, chain := range config.Chains {
		used[chain.Topic] = struct{}{}
	}
	for _, chain := range config.Chains {
		if _, ok := used[chain.LiveTopic]; ok {
			return fmt.Errorf("conf: chain %s live_topic %q is already used", chain.Name, chain.LiveTopic)
		}
		used[chain.LiveTopic] = struct{}{}
	}
	return nil
}

// LiveTopics maps chain IDs to their live topics, or returns nil when live
// push is disabled.
func (config *Config) LiveTopics() map[uint64]string {
	if !config.Live.Enabled {
		return nil
	}
	topics := make(map[uint64]string, len(config.Chains))
	for _, chain := range config.Chains {
		topics[chain.ChainID] = chain.LiveTopic
	}
	return topics
}
