// Package conf is market-rankings' configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const (
	defaultRefreshInterval = time.Minute
	defaultCandleInterval  = 5 * time.Minute
	defaultReorgWindow     = time.Hour
	// maxCandleInterval is the 24h window the candles are summed over.
	maxCandleInterval = 24 * time.Hour
)

// ProviderSet provides the sections of Bootstrap to the injector.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Clickhouse", "Postgres", "Metrics", "Chains", "Rankings"),
)

// Load reads, defaults and validates the configuration at path, a file or a
// directory of them. The returned func closes the config's sources.
func Load(path string) (*Bootstrap, func() error, error) {
	bootstrap := &Bootstrap{}
	closeConfig, err := config.Load(path, bootstrap)
	if err != nil {
		return nil, nil, err
	}
	return bootstrap, closeConfig, nil
}

// Parse decodes, defaults and validates raw YAML without the environment.
func Parse(raw []byte) (*Bootstrap, error) {
	bootstrap := &Bootstrap{}
	if err := config.Parse(raw, bootstrap); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

// ApplyDefaults fills every section and replaces unset settings with their
// defaults.
func (bootstrap *Bootstrap) ApplyDefaults() {
	if bootstrap.Clickhouse == nil {
		bootstrap.Clickhouse = &config.ClickHouse{}
	}
	if bootstrap.Postgres == nil {
		bootstrap.Postgres = &config.Postgres{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	if bootstrap.Rankings == nil {
		bootstrap.Rankings = &Rankings{}
	}
	bootstrap.Clickhouse.ApplyDefaults()

	rankings := bootstrap.Rankings
	config.DefaultDuration(&rankings.RefreshInterval, defaultRefreshInterval)
	config.DefaultDuration(&rankings.CandleInterval, defaultCandleInterval)
	config.DefaultDuration(&rankings.ReorgWindow, defaultReorgWindow)
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
func (bootstrap *Bootstrap) Validate() error {
	if err := bootstrap.GetClickhouse().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetPostgres().Validate(); err != nil {
		return err
	}
	rankings := bootstrap.GetRankings()
	if rankings.GetRefreshInterval().AsDuration() < 0 || rankings.GetReorgWindow().AsDuration() < 0 {
		return errors.New("conf: rankings durations must be positive")
	}
	candleInterval := rankings.GetCandleInterval().AsDuration()
	if candleInterval < time.Second || candleInterval%time.Second != 0 || candleInterval > maxCandleInterval {
		return fmt.Errorf("conf: rankings.candle_interval %s must be whole seconds and at most 24h", candleInterval)
	}
	return validateChains(bootstrap.GetChains())
}

// validateChains checks that there is at least one chain and that each has
// a name and a unique chain_id; market-rankings reads no topic.
func validateChains(chains []*config.Chain) error {
	if len(chains) == 0 {
		return errors.New("conf: at least one chain is required")
	}
	ids := make(map[uint64]struct{}, len(chains))
	for index, chain := range chains {
		if chain.GetName() == "" || chain.GetChainId() == 0 {
			return fmt.Errorf("conf: chains[%d] needs name and chain_id", index)
		}
		if _, ok := ids[chain.GetChainId()]; ok {
			return fmt.Errorf("conf: duplicate chain_id %d", chain.GetChainId())
		}
		ids[chain.GetChainId()] = struct{}{}
	}
	return nil
}
