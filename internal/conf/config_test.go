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
	if config.Holders.ReconcileEvery != defaultHoldersReconcileEvery || config.Holders.RPCBatchSize != defaultRPCBatchSize ||
		config.Holders.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("holders defaults = %+v", config.Holders)
	}
	if config.Registry.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("registry defaults = %+v", config.Registry)
	}
	if config.ChLoader.BatchLag != defaultLoaderBatchLag || config.ChLoader.FlushRows != defaultLoaderFlushRows ||
		config.ChLoader.FlushInterval != defaultLoaderFlushInterval {
		t.Fatalf("ch_loader defaults = %+v", config.ChLoader)
	}
	if config.State.Driver != StateMemory {
		t.Fatalf("state driver = %q", config.State.Driver)
	}
	if config.Rankings.RefreshInterval != defaultRankingsRefreshInterval ||
		config.Rankings.CandleInterval != defaultRankingsCandleInterval ||
		config.Rankings.ReorgWindow != defaultRankingsReorgWindow {
		t.Fatalf("rankings defaults = %+v", config.Rankings)
	}
	if config.Market.MaxHops != defaultMaxHops || len(config.Market.Intervals) != len(DefaultIntervals()) ||
		config.Market.MinLiquidityUSD != defaultMinLiquidityUSD || config.Market.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("market defaults = %+v", config.Market)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]string{
		"no brokers":             "chains: [{name: eth, chain_id: 1, topic: a}]",
		"empty broker":           "kafka: {brokers: ['']}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"no chains":              "kafka: {brokers: [b]}",
		"missing topic":          "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1}]",
		"duplicate id":           "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: base, chain_id: 1, topic: b}]",
		"duplicate name":         "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: eth, chain_id: 2, topic: b}]",
		"duplicate topic":        "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}, {name: base, chain_id: 2, topic: a}]",
		"bad interval":           "kafka: {brokers: [b]}\nmarket: {intervals: [1500ms]}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative liq":           "kafka: {brokers: [b]}\nmarket: {min_liquidity_usd: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"feed no token":          "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a, feeds: [{address: f, decimals: 8}]}]",
		"duplicate feed":         "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a, feeds: [{address: f, token: t}, {address: F, token: T}]}]",
		"feed proxy and address": "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a, rpc_url: r, feeds: [{proxy: p, address: f, token: t}]}]",
		"feed proxy no rpc":      "kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a, feeds: [{proxy: p, token: t}]}]",
		"negative outlier":       "kafka: {brokers: [b]}\nmarket: {outlier_deviation: -0.5}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"clickhouse no addr":     "kafka: {brokers: [b]}\nstorage: {driver: clickhouse}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown driver":         "kafka: {brokers: [b]}\nstorage: {driver: postgres}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"postgres no dsn":        "kafka: {brokers: [b]}\nstate: {driver: postgres}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown state":          "kafka: {brokers: [b]}\nstate: {driver: redis}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative holders":       "kafka: {brokers: [b]}\nholders: {rpc_batch_size: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative registry":      "kafka: {brokers: [b]}\nregistry: {history_depth: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative rankings":      "kafka: {brokers: [b]}\nrankings: {refresh_interval: -1s}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown candle":         "kafka: {brokers: [b]}\nrankings: {candle_interval: 2m}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative ch_loader":     "kafka: {brokers: [b]}\nch_loader: {flush_rows: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"negative live":          "kafka: {brokers: [b]}\nlive: {client_buffer: -1}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"live topic reused":      "kafka: {brokers: [b]}\nlive: {enabled: true}\nchains: [{name: eth, chain_id: 1, topic: a, live_topic: b}, {name: base, chain_id: 2, topic: b}]",
		"live topic twice":       "kafka: {brokers: [b]}\nlive: {enabled: true}\nchains: [{name: eth, chain_id: 1, topic: a, live_topic: l}, {name: base, chain_id: 2, topic: b, live_topic: l}]",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(raw)); err == nil {
				t.Fatal("Parse() error = nil")
			}
		})
	}
}

func TestParseLive(t *testing.T) {
	config, err := Parse([]byte(`
kafka:
  brokers: [127.0.0.1:9092]
live:
  enabled: true
  max_lag: -1s
  allowed_origins: [localhost:5173]
chains:
  - {name: eth, chain_id: 1, topic: chainstream-eth}
  - {name: base, chain_id: 8453, topic: chainstream-base, live_topic: base-push}
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	live := config.Live
	if live.MaxLag != -time.Second || live.CandleThrottle != defaultLiveCandleThrottle ||
		live.ClientBuffer != defaultLiveClientBuffer || live.MaxSubscriptions != defaultLiveMaxSubscriptions ||
		len(live.AllowedOrigins) != 1 {
		t.Fatalf("live = %+v", live)
	}
	topics := config.LiveTopics()
	if len(topics) != 2 || topics[1] != "chainstream-eth-live" || topics[8453] != "base-push" {
		t.Fatalf("LiveTopics() = %v", topics)
	}
	config.Live.Enabled = false
	if topics := config.LiveTopics(); topics != nil {
		t.Fatalf("disabled LiveTopics() = %v", topics)
	}
}

func TestParseFeeds(t *testing.T) {
	config, err := Parse([]byte(`
kafka: {brokers: [b]}
chains:
  - name: eth
    chain_id: 1
    topic: a
    rpc_url: http://node
    feeds:
      - {address: "0xfeed", token: "0xtoken", decimals: 8}
      - {proxy: "0xproxy", token: "0xtoken", description: "ETH / USD"}
      - {proxy: "0xproxy", token: "0xother"}
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	feeds := config.Chains[0].Feeds
	if len(feeds) != 3 || feeds[1].Proxy != "0xproxy" || feeds[1].Description != "ETH / USD" || feeds[2].Token != "0xother" {
		t.Fatalf("feeds = %+v", feeds)
	}
	if feeds[0].Address != "0xfeed" || feeds[0].Token != "0xtoken" || feeds[0].Decimals != 8 {
		t.Fatalf("feeds = %+v", feeds)
	}
	if config.Market.OracleDeviation != defaultOracleDeviation || config.Market.OutlierDeviation != defaultOutlierDeviation {
		t.Fatalf("deviations = %v, %v", config.Market.OracleDeviation, config.Market.OutlierDeviation)
	}
}

func TestParseClickHouseStorage(t *testing.T) {
	config, err := Parse([]byte(`
kafka: {brokers: [b]}
storage:
  driver: clickhouse
  clickhouse: {addr: ["127.0.0.1:9000"]}
chains: [{name: eth, chain_id: 1, topic: a}]
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if config.Storage.ClickHouse.Database != defaultClickHouseDatabase {
		t.Fatalf("database = %q", config.Storage.ClickHouse.Database)
	}
}

func TestParsePostgresState(t *testing.T) {
	config, err := Parse([]byte(`
kafka: {brokers: [b]}
state:
  driver: postgres
  postgres: {dsn: "postgres://market@127.0.0.1:5432/market", schema: engine}
chains: [{name: eth, chain_id: 1, topic: a}]
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if config.State.Postgres.DSN != "postgres://market@127.0.0.1:5432/market" || config.State.Postgres.Schema != "engine" {
		t.Fatalf("state = %+v", config.State)
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
