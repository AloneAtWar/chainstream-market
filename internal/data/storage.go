package data

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/conf"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chraw"
	"github.com/AloneAtWar/chainstream-market/internal/storage/chstore"
	"github.com/AloneAtWar/chainstream-market/internal/storage/pgstore"
)

// Storage backends are chosen by name in the config. storage.driver opens
// the market backend, which market-engine writes and market-api and
// market-rankings read, and the raw backend ch-loader writes; state.driver
// opens the state backend, which keeps market-engine's state and the tables
// of holder-indexer, market-registry and market-rankings.
//
// A backend serves every storage role, such as storage.Output or
// storage.HolderStore, that it implements, and the commands depend on roles
// rather than backends. Adding a backend means implementing the roles it
// serves and listing its opener below; a command whose role the configured
// driver does not serve fails at startup, naming the driver and the role.

// Opener opens a backend from its config section. A nil store serves no
// role, such as state kept in memory.
type Opener[Config any] func(ctx context.Context, config Config, logger *slog.Logger) (store any, cleanup func(), err error)

var (
	marketDrivers = map[string]Opener[*conf.Storage]{
		conf.StorageMemory:     openMemory,
		conf.StorageClickHouse: openClickHouse,
	}
	rawDrivers = map[string]Opener[*conf.Storage]{
		conf.StorageClickHouse: openClickHouseRaw,
	}
	stateDrivers = map[string]Opener[*conf.State]{
		conf.StateMemory:   openMemoryState,
		conf.StatePostgres: openPostgres,
	}
)

// Backend is a store opened by the driver a config setting names.
type Backend struct {
	// Setting is the config key that chose the driver, such as
	// storage.driver.
	Setting string
	Driver  string
	store   any
}

// The backends of each config setting, as distinct types for wire.
type (
	MarketBackend struct{ Backend }
	RawBackend    struct{ Backend }
	StateBackend  struct{ Backend }
)

func openBackend[Config any](
	ctx context.Context, kind, setting, driver string, drivers map[string]Opener[Config], config Config, logger *slog.Logger,
) (Backend, func(), error) {
	opener, ok := drivers[driver]
	if !ok {
		return Backend{}, nil, fmt.Errorf("%s %q has no %s backend; use one of: %s", setting, driver, kind, driverNames(drivers))
	}
	store, cleanup, err := opener(ctx, config, logger)
	if err != nil {
		return Backend{}, nil, fmt.Errorf("%s %q: %w", setting, driver, err)
	}
	return Backend{Setting: setting, Driver: driver, store: store}, cleanup, nil
}

// driverNames lists the drivers of a setting.
func driverNames[Driver any](drivers map[string]Driver) string {
	return strings.Join(slices.Sorted(maps.Keys(drivers)), ", ")
}

// NewMarketBackend opens the market backend storage.driver names.
func NewMarketBackend(ctx context.Context, config *conf.Storage, logger *slog.Logger) (*MarketBackend, func(), error) {
	backend, cleanup, err := openBackend(ctx, "market", "storage.driver", config.GetDriver(), marketDrivers, config, logger)
	if err != nil {
		return nil, nil, err
	}
	return &MarketBackend{backend}, cleanup, nil
}

// NewRawBackend opens the raw backend storage.driver names.
func NewRawBackend(ctx context.Context, config *conf.Storage, logger *slog.Logger) (*RawBackend, func(), error) {
	backend, cleanup, err := openBackend(ctx, "raw", "storage.driver", config.GetDriver(), rawDrivers, config, logger)
	if err != nil {
		return nil, nil, err
	}
	return &RawBackend{backend}, cleanup, nil
}

// NewStateBackend opens the state backend state.driver names.
func NewStateBackend(ctx context.Context, config *conf.State, logger *slog.Logger) (*StateBackend, func(), error) {
	backend, cleanup, err := openBackend(ctx, "state", "state.driver", config.GetDriver(), stateDrivers, config, logger)
	if err != nil {
		return nil, nil, err
	}
	return &StateBackend{backend}, cleanup, nil
}

// Role returns backend's store as Role, or an error naming the setting and
// driver when the store does not serve it.
func Role[Role any](backend Backend) (Role, error) {
	role, ok := backend.store.(Role)
	if !ok {
		return role, fmt.Errorf("%s %q does not provide %s", backend.Setting, backend.Driver, reflect.TypeFor[Role]())
	}
	return role, nil
}

// OptionalRole returns backend's store as Role, or the zero Role when the
// store does not serve it.
func OptionalRole[Role any](backend Backend) Role {
	role, _ := backend.store.(Role)
	return role
}

// retainer ages rows out by storage.clickhouse.retention.
type retainer interface {
	ApplyRetention(ctx context.Context, retention chstore.Retention) error
}

// retain applies storage.clickhouse.retention to store when it ages rows
// out.
func retain(ctx context.Context, store any, config *conf.Storage) error {
	if store, ok := store.(retainer); ok {
		return store.ApplyRetention(ctx, Retention(config.GetClickhouse().GetRetention()))
	}
	return nil
}

// NewOutput returns market-engine's output and applies the backend's
// retention.
func NewOutput(ctx context.Context, backend *MarketBackend, config *conf.Storage) (storage.Output, error) {
	output, err := Role[storage.Output](backend.Backend)
	if err != nil {
		return nil, err
	}
	if err := retain(ctx, output, config); err != nil {
		return nil, err
	}
	return output, nil
}

// NewReader returns what market-api reads market-engine's output through.
func NewReader(backend *MarketBackend) (storage.Reader, error) {
	return Role[storage.Reader](backend.Backend)
}

// NewRankingSource returns what market-rankings reads market-engine's
// output through.
func NewRankingSource(backend *MarketBackend) (storage.RankingSource, error) {
	return Role[storage.RankingSource](backend.Backend)
}

// NewRawStore returns ch-loader's raw store and applies the backend's
// retention.
func NewRawStore(ctx context.Context, backend *RawBackend, config *conf.Storage) (storage.RawStore, error) {
	store, err := Role[storage.RawStore](backend.Backend)
	if err != nil {
		return nil, err
	}
	if err := retain(ctx, store, config); err != nil {
		return nil, err
	}
	return store, nil
}

// NewTransferReader returns what market-api reads ch-loader's transfers
// through.
func NewTransferReader(backend *RawBackend) (storage.TransferReader, error) {
	return Role[storage.TransferReader](backend.Backend)
}

// historian is a raw backend market-engine's backfills can read.
type historian interface {
	History(wait time.Duration) storage.History
}

// NewHistory returns ch-loader's raw trades for market-engine's backfills,
// or nil when backfills are off or storage.driver has no raw backend that
// serves them. It opens its own raw backend, so market-engine only touches
// ch-loader's tables when it backfills.
func NewHistory(ctx context.Context, config *conf.Storage, market *conf.Market, logger *slog.Logger) (storage.History, func(), error) {
	if _, ok := rawDrivers[config.GetDriver()]; !ok || market.GetBackfillBlocks() <= 0 {
		return nil, func() {}, nil
	}
	backend, cleanup, err := NewRawBackend(ctx, config, logger)
	if err != nil {
		return nil, nil, err
	}
	store := OptionalRole[historian](backend.Backend)
	if store == nil {
		cleanup()
		return nil, func() {}, nil
	}
	return store.History(market.GetBackfillWait().AsDuration()), cleanup, nil
}

// NewStateStore returns the persistent state store, or nil when
// state.driver keeps state in memory.
func NewStateStore(backend *StateBackend) storage.StateStore {
	return OptionalRole[storage.StateStore](backend.Backend)
}

// NewHolderStore returns holder-indexer's store.
func NewHolderStore(backend *StateBackend) (storage.HolderStore, error) {
	return Role[storage.HolderStore](backend.Backend)
}

// NewRegistryStore returns market-registry's store.
func NewRegistryStore(backend *StateBackend) (storage.RegistryStore, error) {
	return Role[storage.RegistryStore](backend.Backend)
}

// NewRankingStore returns market-rankings' store.
func NewRankingStore(backend *StateBackend) (storage.RankingStore, error) {
	return Role[storage.RankingStore](backend.Backend)
}

// StateReaders are what market-api reads from the state backend. Each is
// nil when the backend does not serve it, and market-api leaves its routes
// out.
type StateReaders struct {
	Holders  storage.HolderReader
	Registry storage.RegistryReader
	Rankings storage.RankingReader
}

// NewStateReaders returns the state backend's readers it serves.
func NewStateReaders(backend *StateBackend) StateReaders {
	return StateReaders{
		Holders:  OptionalRole[storage.HolderReader](backend.Backend),
		Registry: OptionalRole[storage.RegistryReader](backend.Backend),
		Rankings: OptionalRole[storage.RankingReader](backend.Backend),
	}
}

func openMemory(context.Context, *conf.Storage, *slog.Logger) (any, func(), error) {
	return storage.NewMemory(), func() {}, nil
}

// openMemoryState serves no role: market-engine keeps its state in memory.
func openMemoryState(context.Context, *conf.State, *slog.Logger) (any, func(), error) {
	return nil, func() {}, nil
}

// clickHouseOptions turns storage.clickhouse into store options.
func clickHouseOptions(config *conf.Storage, logger *slog.Logger) chstore.Options {
	clickhouse := config.GetClickhouse()
	return chstore.Options{
		Addr: clickhouse.GetAddr(), Database: clickhouse.GetDatabase(),
		Username: clickhouse.GetUsername(), Password: clickhouse.GetPassword(),
		DialTimeout: clickhouse.GetDialTimeout().AsDuration(), Logger: logger,
	}
}

func openClickHouse(ctx context.Context, config *conf.Storage, logger *slog.Logger) (any, func(), error) {
	store, err := chstore.Open(ctx, clickHouseOptions(config, logger))
	if err != nil {
		return nil, nil, err
	}
	return store, closer(store.Close, logger, "clickhouse"), nil
}

func openClickHouseRaw(ctx context.Context, config *conf.Storage, logger *slog.Logger) (any, func(), error) {
	store, err := chraw.Open(ctx, clickHouseOptions(config, logger))
	if err != nil {
		return nil, nil, err
	}
	return store, closer(store.Close, logger, "clickhouse raw"), nil
}

func openPostgres(ctx context.Context, config *conf.State, logger *slog.Logger) (any, func(), error) {
	store, err := pgstore.Open(ctx, pgstore.Options{
		DSN: config.GetPostgres().GetDsn(), Schema: config.GetPostgres().GetSchema(), Logger: logger,
	})
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}
