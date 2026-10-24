// Package conf loads the market-engine YAML configuration.
package conf

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the market-engine process configuration.
type Config struct {
	Kafka   Kafka   `yaml:"kafka"`
	Storage Storage `yaml:"storage"`
	Chains  []Chain `yaml:"chains"`
}

// Kafka configures the per-chain topic consumers.
type Kafka struct {
	Brokers  []string `yaml:"brokers"`
	ClientID string   `yaml:"client_id"`
	// FetchMaxBytes must be at least chainstream-evm's max_message_bytes, or a
	// large block can never be fetched.
	FetchMaxBytes int32         `yaml:"fetch_max_bytes"`
	FetchMaxWait  time.Duration `yaml:"fetch_max_wait"`
}

// Storage selects the store implementations.
type Storage struct {
	// Driver is "memory" until the ClickHouse and Redis stores land.
	Driver string `yaml:"driver"`
}

// Chain is one chainstream-evm topic consumed in order by its own engine.
type Chain struct {
	Name    string `yaml:"name"`
	ChainID uint64 `yaml:"chain_id"`
	Topic   string `yaml:"topic"`
}

const (
	StorageMemory = "memory"

	defaultFetchMaxBytes = 104857600
	defaultFetchMaxWait  = 500 * time.Millisecond
)

// Load reads, defaults and validates the configuration file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("conf: read %s: %w", path, err)
	}
	return Parse(raw)
}

// Parse decodes, defaults and validates raw YAML.
func Parse(raw []byte) (*Config, error) {
	var config Config
	if err := yaml.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("conf: decode: %w", err)
	}
	config.applyDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (config *Config) applyDefaults() {
	if config.Kafka.FetchMaxBytes == 0 {
		config.Kafka.FetchMaxBytes = defaultFetchMaxBytes
	}
	if config.Kafka.FetchMaxWait == 0 {
		config.Kafka.FetchMaxWait = defaultFetchMaxWait
	}
	if strings.TrimSpace(config.Storage.Driver) == "" {
		config.Storage.Driver = StorageMemory
	}
}

// Validate reports the first invalid setting.
func (config *Config) Validate() error {
	if len(config.Kafka.Brokers) == 0 {
		return errors.New("conf: kafka.brokers is required")
	}
	for _, broker := range config.Kafka.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("conf: kafka.brokers must not contain empty values")
		}
	}
	if config.Kafka.FetchMaxBytes < 0 || config.Kafka.FetchMaxWait < 0 {
		return errors.New("conf: kafka fetch limits must be positive")
	}
	if config.Storage.Driver != StorageMemory {
		return fmt.Errorf("conf: unsupported storage.driver %q", config.Storage.Driver)
	}
	if len(config.Chains) == 0 {
		return errors.New("conf: at least one chain is required")
	}
	names := make(map[string]struct{}, len(config.Chains))
	ids := make(map[uint64]struct{}, len(config.Chains))
	topics := make(map[string]struct{}, len(config.Chains))
	for index, chain := range config.Chains {
		if strings.TrimSpace(chain.Name) == "" || chain.ChainID == 0 || strings.TrimSpace(chain.Topic) == "" {
			return fmt.Errorf("conf: chains[%d] needs name, chain_id and topic", index)
		}
		if _, ok := names[chain.Name]; ok {
			return fmt.Errorf("conf: duplicate chain name %q", chain.Name)
		}
		if _, ok := ids[chain.ChainID]; ok {
			return fmt.Errorf("conf: duplicate chain_id %d", chain.ChainID)
		}
		if _, ok := topics[chain.Topic]; ok {
			return fmt.Errorf("conf: duplicate topic %q", chain.Topic)
		}
		names[chain.Name], ids[chain.ChainID], topics[chain.Topic] = struct{}{}, struct{}{}, struct{}{}
	}
	return nil
}
