package conf

import (
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `
clickhouse: {addr: [127.0.0.1:9000]}
postgres: {dsn: postgres://market@127.0.0.1/market}
chains: [{name: eth, chain_id: 1}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	rankings := bootstrap.Rankings
	if rankings.RefreshInterval.AsDuration() != defaultRefreshInterval ||
		rankings.CandleInterval.AsDuration() != defaultCandleInterval || rankings.ReorgWindow.AsDuration() != defaultReorgWindow {
		t.Fatalf("rankings = %+v", rankings)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"no postgres": {
			"clickhouse: {addr: [c]}\nchains: [{name: eth, chain_id: 1, topic: a}]", "postgres.dsn is required",
		},
		"no clickhouse": {
			"postgres: {dsn: x}\nchains: [{name: eth, chain_id: 1, topic: a}]", "clickhouse.addr is required",
		},
		"long candle interval": {
			minimal + "rankings: {candle_interval: 48h}", "at most 24h",
		},
		"duplicate chain": {
			"clickhouse: {addr: [c]}\npostgres: {dsn: x}\nchains: [{name: eth, chain_id: 1}, {name: eth2, chain_id: 1}]", "duplicate chain_id",
		},
		"negative reorg window": {
			minimal + "rankings: {reorg_window: -1m}", "rankings durations must be positive",
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
		filepath.Join("..", "..", "..", "..", "configs", "market-rankings.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "market-rankings.yaml"),
	} {
		bootstrap, closeConfig, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", path, err)
		}
		if err := closeConfig(); err != nil {
			t.Fatal(err)
		}
		if bootstrap.Postgres.Dsn == "" || len(bootstrap.Clickhouse.Addr) == 0 || len(bootstrap.Chains) == 0 {
			t.Fatalf("%s = %+v", path, bootstrap)
		}
	}
}
