// Package registry keeps the latest token and pool metadata from a chain
// topic's asset and pool facts.
//
// A fact replaces the stored metadata unless the stored metadata was read at
// a higher source height, so a late observation of stale metadata does not
// undo a newer one. Whether a contract was created, and its deployment, are
// kept whatever the height. Every block records the rows it touched as they
// were before it, so a reorg restores them.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/engine"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

// ErrReorgTooDeep means a revert reaches below the retained undo history.
var ErrReorgTooDeep = errors.New("registry: reorg deeper than retained history")

type Options struct {
	ChainID uint64
	Store   storage.RegistryStore
	// HistoryDepth is how many blocks of undo history to keep in addition
	// to everything above the finalized height.
	HistoryDepth int64
	Logger       *slog.Logger
}

// Registry is an engine.Handler for one chain. It is not safe for concurrent
// use.
type Registry struct {
	options       Options
	logger        *slog.Logger
	pending       storage.RegistryChange
	prunedThrough int64
}

func New(options Options) (*Registry, error) {
	if options.ChainID == 0 || options.Store == nil {
		return nil, errors.New("registry: chain id and store are required")
	}
	if options.HistoryDepth <= 0 {
		return nil, errors.New("registry: history depth must be positive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{options: options, logger: logger}, nil
}

func (registry *Registry) Restore(ctx context.Context, chainID uint64) (storage.Checkpoint, bool, error) {
	checkpoint, prunedThrough, ok, err := registry.options.Store.LoadRegistryCheckpoint(ctx, chainID)
	if err != nil || !ok {
		return storage.Checkpoint{}, false, err
	}
	registry.prunedThrough = prunedThrough
	return checkpoint, true, nil
}

func (registry *Registry) Apply(_ context.Context, block *streamv1.BlockEvent) error {
	height := block.GetBlockHeight()
	change := storage.RegistryChange{Height: height}
	tokens := make(map[string]int)
	for _, fact := range block.GetAssets() {
		token, ok := registry.token(fact, height)
		if !ok {
			continue
		}
		if index, seen := tokens[token.Address]; seen {
			change.Tokens[index] = mergeTokens(change.Tokens[index], token)
			continue
		}
		tokens[token.Address] = len(change.Tokens)
		change.Tokens = append(change.Tokens, token)
	}
	pools := make(map[string]int)
	for _, fact := range block.GetPools() {
		pool, ok := registry.pool(fact, height)
		if !ok {
			continue
		}
		if index, seen := pools[pool.PoolID]; seen {
			change.Pools[index] = mergePools(change.Pools[index], pool)
			continue
		}
		pools[pool.PoolID] = len(change.Pools)
		change.Pools = append(change.Pools, pool)
	}

	through := block.GetFinalizedHeight()
	if byDepth := height - registry.options.HistoryDepth; byDepth > through {
		through = byDepth
	}
	registry.prunedThrough = max(registry.prunedThrough, through)
	registry.pending = change
	return nil
}

func (registry *Registry) Revert(_ context.Context, rollbackTo int64) error {
	if rollbackTo < registry.prunedThrough {
		return fmt.Errorf("%w: rollback to %d, history starts after %d", ErrReorgTooDeep, rollbackTo, registry.prunedThrough)
	}
	registry.pending = storage.RegistryChange{Reverted: true, RevertTo: rollbackTo}
	return nil
}

func (registry *Registry) Commit(ctx context.Context, checkpoint storage.Checkpoint) error {
	change := registry.pending
	change.PrunedThrough = registry.prunedThrough
	if err := registry.options.Store.CommitRegistry(ctx, checkpoint, change); err != nil {
		return err
	}
	registry.pending = storage.RegistryChange{}
	return nil
}

func (registry *Registry) token(fact *streamv1.AssetFact, height int64) (storage.Token, bool) {
	address := normalize(fact.GetAddress())
	if address == "" {
		return storage.Token{}, false
	}
	token := storage.Token{
		Address: address, Standard: fact.GetStandard(), Recognition: fact.GetRecognition(),
		MetadataComplete: fact.GetMetadataComplete(),
		Created:          fact.GetType() == streamv1.AssetFactType_ASSET_FACT_TYPE_CREATED,
		SourceHeight:     sourceHeight(fact.GetSourceHeight(), height),
	}
	if deployment := fact.GetDeployment(); deployment != nil {
		token.Created = true
		token.Deployment = &storage.Deployment{
			BlockHeight: deployment.GetBlockHeight(), BlockTime: deployment.GetBlockTime(),
			TransactionIndex: deployment.GetTransactionIndex(),
			TransactionHash:  normalize(deployment.GetTransactionHash()), TraceID: deployment.GetTraceId(),
		}
	}
	token.Metadata = registry.cleanJSON(fact.GetMetadataJson(), "asset", address)
	token.Name, token.Symbol, token.Decimals = ParseMetadata(token.Metadata)
	return token, true
}

func (registry *Registry) pool(fact *streamv1.PoolFact, height int64) (storage.Pool, bool) {
	poolID := normalize(fact.GetPoolId())
	if poolID == "" {
		return storage.Pool{}, false
	}
	pool := storage.Pool{
		PoolID: poolID, Protocol: fact.GetProtocol(), DexAddress: normalize(fact.GetDexAddress()),
		Tokens: make([]string, len(fact.GetTokenAddresses())), Created: fact.GetCreated(),
		SourceHeight: sourceHeight(fact.GetSourceHeight(), height), SourceTime: fact.GetSourceTime(),
		Metadata: registry.cleanJSON(fact.GetMetadataJson(), "pool", poolID),
	}
	for index, token := range fact.GetTokenAddresses() {
		pool.Tokens[index] = normalize(token)
	}
	return pool, true
}

// cleanJSON returns metadata that PostgreSQL accepts as JSONB, or nil.
// JSONB rejects the NUL character, which a token can put in its name.
func (registry *Registry) cleanJSON(raw []byte, kind, key string) []byte {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if !json.Valid(raw) {
		registry.logger.Warn("dropping invalid metadata", "kind", kind, "key", key)
		return nil
	}
	if !bytes.Contains(raw, []byte(`\u0000`)) {
		return raw
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	cleaned, err := json.Marshal(stripNUL(value))
	if err != nil {
		return nil
	}
	return cleaned
}

func stripNUL(value any) any {
	switch typed := value.(type) {
	case string:
		return strings.ReplaceAll(typed, "\x00", "")
	case []any:
		for index, item := range typed {
			typed[index] = stripNUL(item)
		}
	case map[string]any:
		cleaned := make(map[string]any, len(typed))
		for key, item := range typed {
			cleaned[strings.ReplaceAll(key, "\x00", "")] = stripNUL(item)
		}
		return cleaned
	}
	return value
}

// ParseMetadata reads name, symbol and decimals from chainstream-evm's
// asset.Info JSON. ERC-20 and ERC-721 fields are {"Value": v, "Known": b}
// and only known values count; native assets use plain values.
func ParseMetadata(raw []byte) (name, symbol *string, decimals *uint8) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil {
		return nil, nil, nil
	}
	var nameValue, symbolValue string
	var decimalsValue uint8
	if field(fields["Name"], &nameValue) {
		name = &nameValue
	}
	if field(fields["Symbol"], &symbolValue) {
		symbol = &symbolValue
	}
	if field(fields["Decimals"], &decimalsValue) {
		decimals = &decimalsValue
	}
	return name, symbol, decimals
}

func field[T any](raw json.RawMessage, target *T) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	if raw[0] != '{' {
		return json.Unmarshal(raw, target) == nil
	}
	var optional struct {
		Value json.RawMessage
		Known bool
	}
	if json.Unmarshal(raw, &optional) != nil || !optional.Known {
		return false
	}
	return json.Unmarshal(optional.Value, target) == nil
}

// mergeTokens combines two facts about one address in a block: the later
// metadata wins unless it was read at a lower height, and a deployment from
// either is kept.
func mergeTokens(earlier, later storage.Token) storage.Token {
	merged := later
	if earlier.SourceHeight > later.SourceHeight {
		merged = earlier
	}
	merged.Created = earlier.Created || later.Created
	if merged.Deployment == nil {
		merged.Deployment = earlier.Deployment
		if merged.Deployment == nil {
			merged.Deployment = later.Deployment
		}
	}
	return merged
}

func mergePools(earlier, later storage.Pool) storage.Pool {
	merged := later
	if earlier.SourceHeight > later.SourceHeight {
		merged = earlier
	}
	merged.Created = earlier.Created || later.Created
	if merged.Metadata == nil {
		merged.Metadata = earlier.Metadata
		if merged.Metadata == nil {
			merged.Metadata = later.Metadata
		}
	}
	return merged
}

// sourceHeight falls back to the block height for facts that carry none.
func sourceHeight(source, height int64) int64 {
	if source <= 0 {
		return height
	}
	return source
}

func normalize(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

var _ engine.Handler = (*Registry)(nil)
