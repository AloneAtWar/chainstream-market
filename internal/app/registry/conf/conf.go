// Package conf is market-registry's configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"errors"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const defaultHistoryDepth = 256

// ProviderSet provides the sections of Bootstrap to the injector.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Source", "Kafka", "Postgres", "Metrics", "Chains", "Registry"),
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
	if bootstrap.Postgres == nil {
		bootstrap.Postgres = &config.Postgres{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	if bootstrap.Registry == nil {
		bootstrap.Registry = &Registry{}
	}
	bootstrap.Source.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()
	if bootstrap.Registry.HistoryDepth == 0 {
		bootstrap.Registry.HistoryDepth = defaultHistoryDepth
	}
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
func (bootstrap *Bootstrap) Validate() error {
	if err := bootstrap.GetSource().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetKafka().Validate(bootstrap.GetSource().NeedsKafka()); err != nil {
		return err
	}
	if err := bootstrap.GetPostgres().Validate(); err != nil {
		return err
	}
	if bootstrap.GetRegistry().GetHistoryDepth() < 0 {
		return errors.New("conf: registry.history_depth must be positive")
	}
	return config.ValidateChains(bootstrap.GetChains(), nil)
}
