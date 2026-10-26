package conf

import (
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `
kafka: {brokers: [127.0.0.1:9092]}
postgres: {dsn: postgres://market@127.0.0.1/market}
chains: [{name: eth, chain_id: 1, topic: chainstream-eth}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if bootstrap.Registry.HistoryDepth != defaultHistoryDepth {
		t.Fatalf("history_depth = %d", bootstrap.Registry.HistoryDepth)
	}
	if bootstrap.Source.Driver != "kafka" || bootstrap.Kafka.FetchMaxBytes == 0 {
		t.Fatalf("source = %+v, kafka = %+v", bootstrap.Source, bootstrap.Kafka)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"no postgres": {
			"kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}]", "postgres.dsn is required",
		},
		"no brokers": {
			"postgres: {dsn: x}\nchains: [{name: eth, chain_id: 1, topic: a}]", "kafka.brokers is required",
		},
		"negative history": {
			minimal + "registry: {history_depth: -1}", "registry.history_depth",
		},
		"no chains": {
			"kafka: {brokers: [b]}\npostgres: {dsn: x}", "at least one chain",
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
		filepath.Join("..", "..", "..", "..", "configs", "market-registry.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "market-registry.yaml"),
	} {
		bootstrap, closeConfig, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", path, err)
		}
		if err := closeConfig(); err != nil {
			t.Fatal(err)
		}
		if bootstrap.Postgres.Dsn == "" || len(bootstrap.Chains) == 0 {
			t.Fatalf("%s = %+v", path, bootstrap)
		}
	}
}
