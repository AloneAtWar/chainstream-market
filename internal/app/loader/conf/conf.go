// Package conf is ch-loader's configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"errors"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const (
	defaultBatchLag      = 5 * time.Minute
	defaultFlushRows     = 100000
	defaultFlushInterval = 2 * time.Second
)

// ProviderSet provides the sections of Bootstrap to the injector.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Source", "Kafka", "Clickhouse", "Metrics", "Chains", "Loader"),
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
	if bootstrap.Source == nil {
		bootstrap.Source = &config.Source{}
	}
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &config.Kafka{}
	}
	if bootstrap.Clickhouse == nil {
		bootstrap.Clickhouse = &config.ClickHouse{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	if bootstrap.Loader == nil {
		bootstrap.Loader = &Loader{}
	}
	bootstrap.Source.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()
	bootstrap.Clickhouse.ApplyDefaults()

	loader := bootstrap.Loader
	config.DefaultDuration(&loader.BatchLag, defaultBatchLag)
	if loader.FlushRows == 0 {
		loader.FlushRows = defaultFlushRows
	}
	config.DefaultDuration(&loader.FlushInterval, defaultFlushInterval)
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
func (bootstrap *Bootstrap) Validate() error {
	if err := bootstrap.GetSource().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetKafka().Validate(bootstrap.GetSource().NeedsKafka()); err != nil {
		return err
	}
	if err := bootstrap.GetClickhouse().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetClickhouse().GetRetention().Validate(nil); err != nil {
		return err
	}
	loader := bootstrap.GetLoader()
	if loader.GetBatchLag().AsDuration() < 0 || loader.GetFlushRows() < 0 || loader.GetFlushInterval().AsDuration() < 0 {
		return errors.New("conf: loader settings must be positive")
	}
	return config.ValidateChains(bootstrap.GetChains(), nil)
}
