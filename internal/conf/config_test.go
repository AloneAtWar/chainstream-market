package conf

import (
	"os"
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
	if config.Kafka.FetchMaxBytes != defaultFetchMaxBytes || config.Kafka.FetchMaxWait.AsDuration() != defaultFetchMaxWait {
		t.Fatalf("kafka defaults = %+v", config.Kafka)
	}
	if config.Storage.Driver != StorageMemory {
		t.Fatalf("storage driver = %q", config.Storage.Driver)
	}
	if config.Source.Driver != SourceKafka || config.Live.Driver != LiveKafka {
		t.Fatalf("source driver = %q, live driver = %q", config.Source.Driver, config.Live.Driver)
	}
	if config.Holders.ReconcileEvery != defaultHoldersReconcileEvery || config.Holders.RpcBatchSize != defaultRPCBatchSize ||
		config.Holders.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("holders defaults = %+v", config.Holders)
	}
	if config.Registry.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("registry defaults = %+v", config.Registry)
	}
	if config.ChLoader.BatchLag.AsDuration() != defaultLoaderBatchLag || config.ChLoader.FlushRows != defaultLoaderFlushRows ||
		config.ChLoader.FlushInterval.AsDuration() != defaultLoaderFlushInterval {
		t.Fatalf("ch_loader defaults = %+v", config.ChLoader)
	}
	if config.State.Driver != StateMemory {
		t.Fatalf("state driver = %q", config.State.Driver)
	}
	if config.Rankings.RefreshInterval.AsDuration() != defaultRankingsRefreshInterval ||
		config.Rankings.CandleInterval.AsDuration() != defaultRankingsCandleInterval ||
		config.Rankings.ReorgWindow.AsDuration() != defaultRankingsReorgWindow {
		t.Fatalf("rankings defaults = %+v", config.Rankings)
	}
	if config.Market.MaxHops != defaultMaxHops || len(config.Market.Intervals) != len(DefaultIntervals()) ||
		config.Market.MinLiquidityUsd != defaultMinLiquidityUSD || config.Market.HistoryDepth != defaultHistoryDepth {
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
		"negative retention":     "kafka: {brokers: [b]}\nstorage: {driver: clickhouse, clickhouse: {addr: [c], retention: {trades_days: -1}}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"policy without cold":    "kafka: {brokers: [b]}\nstorage: {driver: clickhouse, clickhouse: {addr: [c], retention: {storage_policy: hot_cold}}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"cold without policy":    "kafka: {brokers: [b]}\nstorage: {driver: clickhouse, clickhouse: {addr: [c], retention: {cold_after_days: 7}}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"retention interval":     "kafka: {brokers: [b]}\nstorage: {driver: clickhouse, clickhouse: {addr: [c], retention: {candle_days: {2m: 30}}}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown source":         "kafka: {brokers: [b]}\nsource: {driver: nats}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"file source no dir":     "source: {driver: file}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"unknown live":           "kafka: {brokers: [b]}\nlive: {driver: nats}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"redis live no addr":     "kafka: {brokers: [b]}\nlive: {enabled: true, driver: redis}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"kafka live no brokers":  "source: {driver: file, file: {dir: d}}\nlive: {enabled: true}\nchains: [{name: eth, chain_id: 1, topic: a}]",
		"rankings candles short": "kafka: {brokers: [b]}\nstorage: {driver: clickhouse, clickhouse: {addr: [c], retention: {candle_days: {5m: 1}}}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
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
	if live.MaxLag.AsDuration() != -time.Second || live.CandleThrottle.AsDuration() != defaultLiveCandleThrottle ||
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

// TestParseWithoutKafka reads chain topics from files and pushes live
// updates through Redis, so no kafka brokers are needed.
func TestParseWithoutKafka(t *testing.T) {
	config, err := Parse([]byte(`
source:
  driver: file
  file: {dir: data}
live:
  enabled: true
  driver: redis
  redis: {addr: 127.0.0.1:6379}
chains:
  - {name: eth, chain_id: 1, topic: chainstream-eth}
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if config.Source.File.PollInterval.AsDuration() != defaultSourcePollInterval ||
		config.Live.Redis.DialTimeout.AsDuration() != defaultRedisDialTimeout {
		t.Fatalf("source = %+v, live = %+v", config.Source, config.Live)
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
	if config.Storage.Clickhouse.Database != defaultClickHouseDatabase {
		t.Fatalf("database = %q", config.Storage.Clickhouse.Database)
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
	if config.State.Postgres.Dsn != "postgres://market@127.0.0.1:5432/market" || config.State.Postgres.Schema != "engine" {
		t.Fatalf("state = %+v", config.State)
	}
}

func TestLoadRepositoryConfig(t *testing.T) {
	config := mustLoad(t, filepath.Join("..", "..", "configs", "config.yaml"))
	if len(config.Chains) != 3 {
		t.Fatalf("chains = %d, want 3", len(config.Chains))
	}
	if config.Kafka.FetchMaxWait.AsDuration() != time.Second/2 {
		t.Fatalf("fetch_max_wait = %s", config.Kafka.FetchMaxWait)
	}
	if days := config.Storage.Clickhouse.Retention.CandleDayDurations(); days[time.Minute] != 30 || days[5*time.Minute] != 180 {
		t.Fatalf("candle_days = %v", days)
	}
}

func TestParseRetention(t *testing.T) {
	config, err := Parse([]byte(`
kafka: {brokers: [b]}
storage:
  driver: clickhouse
  clickhouse:
    addr: [c]
    retention:
      storage_policy: hot_cold
      cold_after_days: 30
      trades_days: 180
      candle_days: {1m: 7, 1h: 0}
chains: [{name: eth, chain_id: 1, topic: a}]
`))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	retention := config.Storage.Clickhouse.Retention
	if retention.ColdVolume != defaultColdVolume || retention.ColdAfterDays != 30 || retention.TradesDays != 180 {
		t.Fatalf("retention = %+v", retention)
	}
	if days := retention.CandleDayDurations(); len(days) != 2 || days[time.Minute] != 7 {
		t.Fatalf("candle_days = %v", retention.CandleDays)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, _, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil || !strings.Contains(err.Error(), "missing.yaml") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadResolvesEnvironment(t *testing.T) {
	t.Setenv("RPC_URL", "http://node:8545")
	t.Setenv("FETCH_WAIT", "2s")
	t.Setenv("LIVE", "true")
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := `kafka:
  brokers: ["${BROKER:127.0.0.1:9092}"]
  fetch_max_wait: ${FETCH_WAIT}
live:
  enabled: ${LIVE:false}
chains:
  - name: eth
    chain_id: 1
    topic: a
    rpc_url: ${RPC_URL}
    live_topic: "${MISSING}"
    feeds: [{address: 0x1f, token: 0x00a1}]
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	config := mustLoad(t, path)
	if config.Kafka.Brokers[0] != "127.0.0.1:9092" || config.Kafka.FetchMaxWait.AsDuration() != 2*time.Second {
		t.Fatalf("kafka = %+v", config.Kafka)
	}
	chain := config.Chains[0]
	if !config.Live.Enabled || chain.RpcUrl != "http://node:8545" || chain.LiveTopic != "a-live" {
		t.Fatalf("chain = %+v, live = %+v", chain, config.Live)
	}
	// Unquoted hex stays text in string fields.
	if feed := chain.Feeds[0]; feed.Address != "0x1f" || feed.Token != "0x00a1" {
		t.Fatalf("feed = %+v", feed)
	}
}

// mustLoad calls Load and closes the config.
func mustLoad(t *testing.T, path string) *Bootstrap {
	t.Helper()
	config, closeConfig, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := closeConfig(); err != nil {
		t.Fatalf("close config: %v", err)
	}
	return config
}

func TestLoadDeployConfig(t *testing.T) {
	t.Setenv("ETH_RPC_URL", "http://node:8545")
	config := mustLoad(t, filepath.Join("..", "..", "deploy", "config.yaml"))
	if config.Storage.Driver != StorageClickHouse || config.State.Driver != StatePostgres || config.Metrics.Addr != ":9100" {
		t.Fatalf("config = %+v", config)
	}
	if len(config.Chains) != 1 || config.Chains[0].RpcUrl != "http://node:8545" {
		t.Fatalf("chains = %+v", config.Chains)
	}
}
