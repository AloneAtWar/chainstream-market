package conf

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const minimal = `
kafka: {brokers: [127.0.0.1:9092]}
clickhouse: {addr: [127.0.0.1:9000]}
postgres: {dsn: postgres://market@127.0.0.1/market}
chains: [{name: eth, chain_id: 1, topic: chainstream-eth}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	wallet := bootstrap.Wallet
	if wallet.HistoryDepth != defaultHistoryDepth || wallet.History.AsDuration() != defaultHistory ||
		wallet.TradesDays != defaultTradesDays || wallet.CloseRatio != defaultCloseRatio {
		t.Fatalf("wallet = %+v", wallet)
	}
	stats := wallet.Stats
	if !slices.Equal(stats.Windows, []int32{7, 30, 90}) || stats.RefreshInterval.AsDuration() != defaultRefreshInterval {
		t.Fatalf("stats = %+v", stats)
	}
	smart := stats.SmartMoney
	if smart.WindowDays != 30 || smart.MinRealizedUsd != 5000 || smart.MinWinRate != 0.5 || smart.MinTokens != 10 ||
		smart.MaxLossShare != 0.3 || smart.MaxWallets != 1000 {
		t.Fatalf("smart money = %+v", smart)
	}
	if bootstrap.Chains[0].LiveTopic != "chainstream-eth-live" {
		t.Fatalf("live topic = %q", bootstrap.Chains[0].LiveTopic)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"history over 90 days": {minimal + "wallet: {history: 2400h}", "at most 90 days"},
		"window over 90 days":  {minimal + "wallet: {stats: {windows: [7, 30, 180]}}", "between 1 and 90"},
		"rule window not a stat window": {
			minimal + "wallet: {stats: {windows: [7, 90]}}", "smart_money.window_days 30 must be one of windows",
		},
		"close ratio": {minimal + "wallet: {close_ratio: 1}", "close_ratio"},
		"win rate":    {minimal + "wallet: {stats: {smart_money: {min_win_rate: 2}}}", "between 0 and 1"},
		"trades kept shorter than a window": {
			minimal + "wallet: {trades_days: 30}", "trades_days must cover",
		},
		"no kafka": {
			"clickhouse: {addr: [c]}\npostgres: {dsn: x}\nchains: [{name: eth, chain_id: 1, topic: a}]", "kafka",
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
		filepath.Join("..", "..", "..", "..", "configs", "market-wallet.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "market-wallet.yaml"),
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
