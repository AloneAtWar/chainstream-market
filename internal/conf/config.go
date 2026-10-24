// Package conf loads the market-engine YAML configuration.
package conf

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the market-engine process configuration.
type Config struct {
	Kafka   Kafka   `yaml:"kafka"`
	Storage Storage `yaml:"storage"`
	Market  Market  `yaml:"market"`
	Chains  []Chain `yaml:"chains"`
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

// Storage selects where market output goes. Checkpoints stay in memory
// either way, so a restart replays each topic from offset zero and the
// ClickHouse store overwrites the heights it already holds.
type Storage struct {
	// Driver is "memory" or "clickhouse".
	Driver     string     `yaml:"driver"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
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

	defaultClickHouseDatabase = "market"

	defaultFetchMaxBytes = 104857600
	defaultFetchMaxWait  = 500 * time.Millisecond

	defaultMinLiquidityUSD       = 1000
	defaultCandleMinLiquidityUSD = 10000
	defaultMaxHops               = 3
	defaultHistoryDepth          = 256
	defaultOracleDeviation       = 0.02
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
	if strings.TrimSpace(config.Storage.Driver) == "" {
		config.Storage.Driver = StorageMemory
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
	return nil
}
