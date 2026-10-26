package conf

import (
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/google/wire"
)

// ProviderSet provides the sections of Bootstrap to the commands' injectors.
// State has a provider of its own: wire matches field names regardless of
// case, so wire.FieldsOf finds the protobuf message's internal state field
// instead.
var ProviderSet = wire.NewSet(
	wire.FieldsOf(new(*Bootstrap),
		"Kafka", "Storage", "Market", "Api", "Live", "Metrics", "Chains"),
	NewState,
)

// NewState provides the state section.
func NewState(bootstrap *Bootstrap) *config.State { return bootstrap.GetState() }
