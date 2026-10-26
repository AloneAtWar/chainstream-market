// Package conf is market-api's configuration: the Bootstrap proto, its
// defaults and its validation.
package conf

import (
	"fmt"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

const defaultAddr = ":8080"

// DefaultIntervals are the candle sizes served when none are configured,
// market-engine's default market.intervals.
func DefaultIntervals() []time.Duration {
	return []time.Duration{
		time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 4 * time.Hour, 24 * time.Hour,
	}
}

// ProviderSet provides the sections of Bootstrap to the injector.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap), "Api", "Clickhouse", "Postgres", "Metrics"),
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
	if bootstrap.Api == nil {
		bootstrap.Api = &API{}
	}
	if bootstrap.Clickhouse == nil {
		bootstrap.Clickhouse = &config.ClickHouse{}
	}
	if bootstrap.Postgres == nil {
		bootstrap.Postgres = &config.Postgres{}
	}
	if bootstrap.Live == nil {
		bootstrap.Live = &config.Live{}
	}
	if bootstrap.Kafka == nil {
		bootstrap.Kafka = &config.Kafka{}
	}
	if bootstrap.Metrics == nil {
		bootstrap.Metrics = &config.Metrics{}
	}
	bootstrap.Clickhouse.ApplyDefaults()
	bootstrap.Live.ApplyDefaults()
	bootstrap.Kafka.ApplyDefaults()
	config.ApplyChainDefaults(bootstrap.Chains)

	api := bootstrap.Api
	if strings.TrimSpace(api.Addr) == "" {
		api.Addr = defaultAddr
	}
	if len(api.Intervals) == 0 {
		for _, interval := range DefaultIntervals() {
			api.Intervals = append(api.Intervals, durationpb.New(interval))
		}
	}
}

// Validate reports the first invalid setting of a defaulted Bootstrap.
func (bootstrap *Bootstrap) Validate() error {
	for _, interval := range bootstrap.GetApi().IntervalDurations() {
		if interval < time.Second || interval%time.Second != 0 {
			return fmt.Errorf("conf: api interval %s must be whole seconds", interval)
		}
	}
	if err := bootstrap.GetClickhouse().Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetPostgres().Validate(); err != nil {
		return err
	}
	live := bootstrap.GetLive()
	if err := live.Validate(); err != nil {
		return err
	}
	if err := bootstrap.GetKafka().Validate(live.NeedsKafka()); err != nil {
		return err
	}
	return config.ValidateChains(bootstrap.GetChains(), live)
}

// IntervalDurations returns the candle intervals served.
func (api *API) IntervalDurations() []time.Duration {
	intervals := make([]time.Duration, len(api.GetIntervals()))
	for index, interval := range api.GetIntervals() {
		intervals[index] = interval.AsDuration()
	}
	return intervals
}
