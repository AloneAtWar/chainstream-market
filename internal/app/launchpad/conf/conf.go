// Package conf is market-launchpad's configuration: the Bootstrap proto, its
// defaults and its validation.
//
// market-launchpad 的配置：Bootstrap proto、默认值与校验。
package conf

import (
	"errors"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

const defaultHistoryDepth = 256

// defaultProtocols are the launchpads chainstream-evm parses.
var defaultProtocols = []string{"fourmeme", "flap", "pons"}

// ProviderSet provides the sections of Bootstrap to the injector.
//
// ProviderSet 把 Bootstrap 的各配置段提供给注入器。
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Source", "Kafka", "Postgres", "Metrics", "Chains", "Launchpad"),
)

// Load reads, defaults and validates the configuration at path, a file or a
// directory of them. The returned func closes the config's sources.
//
// Load 读取、补默认值并校验 path 处的配置。
func Load(path string) (*Bootstrap, func() error, error) {
	bootstrap := &Bootstrap{}
	closeConfig, err := config.Load(path, bootstrap)
	if err != nil {
		return nil, nil, err
	}
	return bootstrap, closeConfig, nil
}

// Parse decodes, defaults and validates raw YAML without the environment.
//
// Parse 解析原始 YAML（不读环境变量），补默认值并校验。
func Parse(raw []byte) (*Bootstrap, error) {
	bootstrap := &Bootstrap{}
	if err := config.Parse(raw, bootstrap); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

// ApplyDefaults fills every section and replaces unset settings with their
// defaults.
//
// ApplyDefaults 补齐各配置段并填入默认值。
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
	if bootstrap.Launchpad == nil {
		bootstrap.Launchpad = &Launchpad{}
	}
	bootstrap.Source.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()
	if bootstrap.Launchpad.HistoryDepth == 0 {
		bootstrap.Launchpad.HistoryDepth = defaultHistoryDepth
	}
	if len(bootstrap.Launchpad.Protocols) == 0 {
		bootstrap.Launchpad.Protocols = append([]string(nil), defaultProtocols...)
	}
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
//
// Validate 返回第一个不合法的配置项。
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
	if bootstrap.GetLaunchpad().GetHistoryDepth() < 0 {
		return errors.New("conf: launchpad.history_depth must be positive")
	}
	for _, protocol := range bootstrap.GetLaunchpad().GetProtocols() {
		if strings.TrimSpace(protocol) == "" {
			return errors.New("conf: launchpad.protocols must not contain an empty name")
		}
	}
	return config.ValidateChains(bootstrap.GetChains(), nil)
}
