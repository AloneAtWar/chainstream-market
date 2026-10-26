// Package conf is holder-indexer's configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const (
	defaultHistoryDepth   = 256
	defaultReconcileEvery = 100
	defaultReconcileBatch = 200
	defaultRPCBatchSize   = 100
	defaultRPCTimeout     = 30 * time.Second
)

// ProviderSet provides the sections of Bootstrap to the injector.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Source", "Kafka", "Postgres", "Metrics", "Chains", "Holders"),
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
	if bootstrap.Holders == nil {
		bootstrap.Holders = &Holders{}
	}
	bootstrap.Source.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()

	holders := bootstrap.Holders
	if holders.HistoryDepth == 0 {
		holders.HistoryDepth = defaultHistoryDepth
	}
	if holders.ReconcileEvery == 0 {
		holders.ReconcileEvery = defaultReconcileEvery
	}
	if holders.ReconcileBatch == 0 {
		holders.ReconcileBatch = defaultReconcileBatch
	}
	if holders.RpcBatchSize == 0 {
		holders.RpcBatchSize = defaultRPCBatchSize
	}
	config.DefaultDuration(&holders.RpcTimeout, defaultRPCTimeout)
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
	holders := bootstrap.GetHolders()
	if holders.GetHistoryDepth() < 0 || holders.GetReconcileBatch() < 0 ||
		holders.GetRpcBatchSize() < 0 || holders.GetRpcTimeout().AsDuration() < 0 {
		return errors.New("conf: holders settings must be positive")
	}
	if err := config.ValidateChains(bootstrap.GetChains(), nil); err != nil {
		return err
	}
	for _, chain := range bootstrap.GetChains() {
		if strings.TrimSpace(chain.GetRpcUrl()) == "" {
			return fmt.Errorf("conf: chain %s needs an rpc_url to read balanceOf", chain.GetName())
		}
	}
	return nil
}
