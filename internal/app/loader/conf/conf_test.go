package conf

import (
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `
kafka: {brokers: [127.0.0.1:9092]}
clickhouse: {addr: [127.0.0.1:9000]}
chains: [{name: eth, chain_id: 1, topic: chainstream-eth}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	loader := bootstrap.Loader
	if loader.BatchLag.AsDuration() != defaultBatchLag || loader.FlushRows != defaultFlushRows ||
		loader.FlushInterval.AsDuration() != defaultFlushInterval {
		t.Fatalf("loader = %+v", loader)
	}
	if bootstrap.Clickhouse.Database != "market" || bootstrap.Clickhouse.Retention.ColdVolume != "cold" {
		t.Fatalf("clickhouse = %+v", bootstrap.Clickhouse)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"no clickhouse": {
			"kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a}]", "clickhouse.addr is required",
		},
		"negative flush rows": {
			minimal + "loader: {flush_rows: -1}", "loader settings must be positive",
		},
		"half a cold tier": {
			"kafka: {brokers: [b]}\nclickhouse: {addr: [c], retention: {cold_after_days: 3}}\nchains: [{name: eth, chain_id: 1, topic: a}]",
			"storage_policy and cold_after_days",
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
		filepath.Join("..", "..", "..", "..", "configs", "ch-loader.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "ch-loader.yaml"),
	} {
		bootstrap, closeConfig, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", path, err)
		}
		if err := closeConfig(); err != nil {
			t.Fatal(err)
		}
		if len(bootstrap.Clickhouse.Addr) == 0 || len(bootstrap.Chains) == 0 {
			t.Fatalf("%s = %+v", path, bootstrap)
		}
	}
}
