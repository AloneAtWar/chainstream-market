package conf

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `
clickhouse: {addr: [127.0.0.1:9000]}
postgres: {dsn: "postgres://market@127.0.0.1:5432/market"}
chains: [{name: eth, chain_id: 1, topic: chainstream-eth}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if bootstrap.Api.Addr != defaultAddr || len(bootstrap.Api.IntervalDurations()) != len(DefaultIntervals()) {
		t.Fatalf("api = %+v", bootstrap.Api)
	}
	if bootstrap.Clickhouse.Database != "market" || bootstrap.Live.ClientBuffer != 1024 {
		t.Fatalf("clickhouse = %+v, live = %+v", bootstrap.Clickhouse, bootstrap.Live)
	}
	if bootstrap.Chains[0].LiveTopic != "chainstream-eth-live" {
		t.Fatalf("chain = %+v", bootstrap.Chains[0])
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"no clickhouse": {
			"postgres: {dsn: d}\nchains: [{name: eth, chain_id: 1, topic: a}]", "clickhouse.addr is required",
		},
		"no postgres": {
			"clickhouse: {addr: [c]}\nchains: [{name: eth, chain_id: 1, topic: a}]", "postgres.dsn is required",
		},
		"bad interval": {
			minimal + "api: {intervals: [1500ms]}", "must be whole seconds",
		},
		"kafka live without brokers": {
			minimal + "live: {enabled: true}", "kafka.brokers is required",
		},
		"no chains": {
			"clickhouse: {addr: [c]}\npostgres: {dsn: d}", "at least one chain",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(test.yaml)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadShippedConfigs(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "..", "..", "..", "configs", "market-api.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "market-api.yaml"),
	} {
		bootstrap, closeConfig, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", path, err)
		}
		if err := closeConfig(); err != nil {
			t.Fatal(err)
		}
		if bootstrap.Postgres.Dsn == "" || len(bootstrap.Clickhouse.Addr) == 0 || len(bootstrap.Chains) == 0 ||
			bootstrap.Api.IntervalDurations()[0] != time.Minute {
			t.Fatalf("%s = %+v", path, bootstrap)
		}
	}
}
