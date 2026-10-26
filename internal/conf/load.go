// Package conf is the configuration every command reads: the Bootstrap
// proto, loaded through internal/pkg/config, plus the defaults and
// validation of the sections that belong to one command.
package conf

import (
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
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
