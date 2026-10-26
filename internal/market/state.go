package market

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// Storage kinds of the processor's journals.
const (
	kindDecimals     = "decimals"
	kindPools        = "pools"
	kindPrices       = "prices"
	kindOrientations = "orientations"
	kindCandles      = "candles"
	kindOracle       = "oracle"
	kindLate         = "late"
	kindMissed       = "missed"
)

// candleKeys encodes a candle key as "subject|interval seconds|key".
var candleKeys = keyCodec[candleKey]{
	kind: kindCandles,
	encode: func(key candleKey) string {
		return fmt.Sprintf("%d|%d|%s", key.Subject, int64(key.Interval/time.Second), key.Key)
	},
	decode: func(raw string) (candleKey, error) {
		parts := strings.SplitN(raw, "|", 3)
		if len(parts) != 3 {
			return candleKey{}, errors.New("want subject|seconds|key")
		}
		subject, err := strconv.ParseUint(parts[0], 10, 8)
		if err != nil {
			return candleKey{}, err
		}
		seconds, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return candleKey{}, err
		}
		return candleKey{
			Subject: storage.CandleSubject(subject), Key: parts[2], Interval: time.Duration(seconds) * time.Second,
		}, nil
	},
}

// TakeDelta returns the state changed since the last call: every key written
// or reverted, with its current value, and the undo history added.
func (processor *Processor) TakeDelta() (storage.StateDelta, error) {
	delta := storage.StateDelta{
		Reverted: processor.reverted, RevertTo: processor.revertTo, PrunedThrough: processor.prunedThrough,
	}
	for _, take := range []func(*storage.StateDelta) error{
		processor.decimals.take, processor.pools.take, processor.prices.take,
		processor.orientations.take, processor.candles.take, processor.oracle.take, processor.late.take,
		processor.missed.take,
	} {
		if err := take(&delta); err != nil {
			return storage.StateDelta{}, err
		}
	}
	processor.reverted, processor.revertTo = false, 0
	return delta, nil
}

// Restore loads persisted state into a processor that has not applied any
// block.
func (processor *Processor) Restore(state storage.State) error {
	if len(processor.pools.values) != 0 || len(processor.decimals.values) != 0 {
		return errors.New("market: restore needs a fresh processor")
	}
	loaders := map[string]struct {
		load func(storage.StateEntry) error
		undo func(storage.StateUndo) error
	}{
		kindDecimals:     {processor.decimals.load, processor.decimals.loadUndo},
		kindPools:        {processor.pools.load, processor.pools.loadUndo},
		kindPrices:       {processor.prices.load, processor.prices.loadUndo},
		kindOrientations: {processor.orientations.load, processor.orientations.loadUndo},
		kindCandles:      {processor.candles.load, processor.candles.loadUndo},
		kindOracle:       {processor.oracle.load, processor.oracle.loadUndo},
		kindLate:         {processor.late.load, processor.late.loadUndo},
		kindMissed:       {processor.missed.load, processor.missed.loadUndo},
	}
	for _, entry := range state.Entries {
		loader, ok := loaders[entry.Kind]
		if !ok {
			return fmt.Errorf("market: unknown state kind %q", entry.Kind)
		}
		if err := loader.load(entry); err != nil {
			return err
		}
	}
	for _, undo := range state.Undo {
		loader, ok := loaders[undo.Kind]
		if !ok {
			return fmt.Errorf("market: unknown state kind %q", undo.Kind)
		}
		if err := loader.undo(undo); err != nil {
			return err
		}
	}
	processor.prunedThrough = state.PrunedThrough
	processor.rebuildLate()

	ids := make([]string, 0, len(processor.pools.values))
	for id := range processor.pools.values {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, member := range processor.pools.values[id].Tokens {
			token := processor.options.Catalog.Canonical(member)
			if !slices.Contains(processor.tokenPools[token], id) {
				processor.tokenPools[token] = append(processor.tokenPools[token], id)
			}
		}
	}
	return nil
}
