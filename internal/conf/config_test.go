package conf

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseAppliesDefaults(t *testing.T) {
	config, err := Parse([]byte(`
kafka:
  brokers: [127.0.0.1:9092]
chains:
  - {name: eth, chain_id: 1, topic: chainstream-eth}
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if config.Kafka.FetchMaxBytes != defaultFetchMaxBytes || config.Kafka.FetchMaxWait != defaultFetchMaxWait {
		t.Fatalf("kafka defaults = %+v", config.Kafka)
	}
	if config.Storage.Driver != StorageMemory {
		t.Fatalf("storage driver = %q", config.Storage.Driver)
	}
	if config.Market.MaxHops != defaultMaxHops || len(config.Market.Intervals) != len(DefaultIntervals()) ||
		config.Market.MinLiquidityUSD != defaultMinLiquidityUSD || config.Market.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("market defaults = %+v", config.Market)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]string{
		"no brokers":      "chains: [{name: eth, chain_id: 1, topic: a}]",
		"empty broker":    "kafka: {brokers: ['']}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"no chains":       "kafka: {brokers: [b]}",
		"missing topic":   "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1}]",
		"duplicate id":    "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: base, chain_id: 1, topic: b}]",
		"duplicate name":  "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: eth, chain_id: 2, topic: b}]",
		"duplicate topic": "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: base, chain_id: 2, topic: a}]",
		"bad interval":    "kafka: {brokers: [b]}\nmarket: {intervals: [1500ms]}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative liq":    "kafka: {brokers: [b]}\nmarket: {min_liquidity_usd: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown driver":  "kafka: {brokers: [b]}\nstorage: {driver: clickhouse}\nchains: [{name: eth, chain_id: 1, topic: a}]",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(raw)); err == nil {
				t.Fatal("Parse() error = nil")
			}
		})
	}
}

func TestLoadRepositoryConfig(t *testing.T) {
	config, err := Load(filepath.Join("..", "..", "configs", "config.yaml"))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(config.Chains) != 3 {
		t.Fatalf("chains = %d, want 3", len(config.Chains))
	}
	if config.Kafka.FetchMaxWait != time.Second/2 {
		t.Fatalf("fetch_max_wait = %s", config.Kafka.FetchMaxWait)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil || !strings.Contains(err.Error(), "read") {
		t.Fatalf("Load() error = %v", err)
	}
}
