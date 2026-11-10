package conf

import (
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `
kafka: {brokers: [127.0.0.1:9092]}
postgres: {dsn: postgres://market@127.0.0.1/market}
chains: [{name: eth, chain_id: 1, topic: chainstream-eth, rpc_url: http://node}]
`

func TestParseAppliesDefaults(t *testing.T) {
	bootstrap, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	holders := bootstrap.Holders
	if holders.HistoryDepth != defaultHistoryDepth || holders.ReconcileEvery != defaultReconcileEvery ||
		holders.ReconcileBatch != defaultReconcileBatch || holders.RpcBatchSize != defaultRPCBatchSize ||
		holders.RpcTimeout.AsDuration() != defaultRPCTimeout || holders.NativeBalances {
		t.Fatalf("holders = %+v", holders)
	}
	if reads := bootstrap.Chains[0].StateReads; reads != "latest" {
		t.Fatalf("state_reads = %q, want latest", reads)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := map[string]struct{ yaml, want string }{
		"no postgres": {
			"kafka: {brokers: [b]}\nchains: [{name: eth, chain_id: 1, topic: a, rpc_url: r}]", "postgres.dsn is required",
		},
		"no rpc_url": {
			"kafka: {brokers: [b]}\npostgres: {dsn: x}\nchains: [{name: eth, chain_id: 1, topic: a}]", "needs an rpc_url",
		},
		"negative batch": {
			minimal + "holders: {rpc_batch_size: -1}", "holders settings must be positive",
		},
		"unknown state reads": {
			"kafka: {brokers: [b]}\npostgres: {dsn: x}\nchains: [{name: eth, chain_id: 1, topic: a, rpc_url: r, state_reads: archive}]",
			"must be latest or exact",
		},
		"exact on TRON": {
			"kafka: {brokers: [b]}\npostgres: {dsn: x}\nchains: [{name: trx, chain_id: 7281264369, topic: a, rpc_url: r, state_reads: exact}]",
			"TRON nodes only serve the latest block",
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
	t.Setenv("ETH_RPC_URL", "http://node:8545")
	for _, path := range []string{
		filepath.Join("..", "..", "..", "..", "configs", "holder-indexer.yaml"),
		filepath.Join("..", "..", "..", "..", "deploy", "configs", "holder-indexer.yaml"),
	} {
		bootstrap, closeConfig, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", path, err)
		}
		if err := closeConfig(); err != nil {
			t.Fatal(err)
		}
		if bootstrap.Postgres.Dsn == "" || len(bootstrap.Chains) == 0 || !bootstrap.Holders.NativeBalances {
			t.Fatalf("%s = %+v", path, bootstrap)
		}
	}
}
