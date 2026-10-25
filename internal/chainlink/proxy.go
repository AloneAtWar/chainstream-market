// Package chainlink reads Chainlink price feed proxies, to find the
// aggregators whose AnswerUpdated logs carry a feed's answers.
package chainlink

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/rpc"
)

// Function selectors read from a proxy.
const (
	selectorDecimals         = "0x313ce567" // decimals()
	selectorDescription      = "0x7284e416" // description()
	selectorAggregator       = "0x245a7bfc" // aggregator()
	selectorPhaseID          = "0x58303b10" // phaseId()
	selectorPhaseAggregators = "0xc1597304" // phaseAggregators(uint16)
)

// maxPhases bounds how many phases are read from one proxy.
const maxPhases = 1000

// Caller runs eth_calls against the latest block, as rpc.Client does.
type Caller interface {
	CallLatest(ctx context.Context, calls []rpc.Call) ([][]byte, error)
}

// Proxy is what a feed proxy reports about itself.
type Proxy struct {
	Address     string
	Description string
	Decimals    uint8
	// Aggregators are every aggregator the proxy has pointed at, oldest
	// phase first and the current one last, lowercased and deduplicated.
	// Each emitted the feed's AnswerUpdated logs while it was current.
	Aggregators []string
}

// Resolve reads a proxy's decimals, description and aggregators. An
// EACAggregatorProxy lists the aggregator of every phase, so a replay from
// an old height trusts the aggregator that was current then. A plain
// AggregatorProxy, without phases, reports only its current aggregator.
func Resolve(ctx context.Context, caller Caller, address string) (Proxy, error) {
	address = strings.ToLower(strings.TrimSpace(address))
	results, err := caller.CallLatest(ctx, []rpc.Call{
		{To: address, Data: selectorDecimals},
		{To: address, Data: selectorDescription},
		{To: address, Data: selectorAggregator},
		{To: address, Data: selectorPhaseID},
	})
	if err != nil {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s: %w", address, err)
	}
	decimals, ok := word(results[0])
	if !ok || !decimals.IsUint64() || decimals.Uint64() > 36 {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no decimals(); is it a feed proxy?", address)
	}
	description, ok := abiString(results[1])
	if !ok {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no description()", address)
	}
	current, ok := addressWord(results[2])
	if !ok {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no aggregator()", address)
	}
	proxy := Proxy{Address: address, Description: description, Decimals: uint8(decimals.Uint64())}

	phase, hasPhases := word(results[3])
	if hasPhases && phase.Sign() > 0 {
		if !phase.IsUint64() || phase.Uint64() > maxPhases {
			return Proxy{}, fmt.Errorf("chainlink: proxy %s reports phase %s", address, phase)
		}
		calls := make([]rpc.Call, phase.Uint64())
		for index := range calls {
			calls[index] = rpc.Call{To: address, Data: fmt.Sprintf("%s%064x", selectorPhaseAggregators, index+1)}
		}
		phases, err := caller.CallLatest(ctx, calls)
		if err != nil {
			return Proxy{}, fmt.Errorf("chainlink: proxy %s phases: %w", address, err)
		}
		for _, result := range phases {
			if aggregator, ok := addressWord(result); ok {
				proxy.add(aggregator)
			}
		}
	}
	proxy.add(current)
	return proxy, nil
}

// add appends aggregator unless it is listed already. The current
// aggregator is added last, so it moves to the end if a phase listed it.
func (proxy *Proxy) add(aggregator string) {
	for index, existing := range proxy.Aggregators {
		if existing == aggregator {
			proxy.Aggregators = append(proxy.Aggregators[:index], proxy.Aggregators[index+1:]...)
			break
		}
	}
	proxy.Aggregators = append(proxy.Aggregators, aggregator)
}

// word reads the first 32-byte word of return data.
func word(data []byte) (*big.Int, bool) {
	if len(data) < 32 {
		return nil, false
	}
	return new(big.Int).SetBytes(data[:32]), true
}

// addressWord reads a non-zero address from the first word of return data.
func addressWord(data []byte) (string, bool) {
	value, ok := word(data)
	if !ok || value.Sign() == 0 || value.BitLen() > 160 {
		return "", false
	}
	return fmt.Sprintf("0x%040x", value), true
}

// abiString decodes return data holding a single ABI-encoded string.
func abiString(data []byte) (string, bool) {
	offset, ok := word(data)
	if !ok || !offset.IsUint64() || offset.Uint64() > uint64(len(data)) {
		return "", false
	}
	start := offset.Uint64()
	length, ok := word(data[start:])
	if !ok || !length.IsUint64() || length.Uint64() > uint64(len(data))-start-32 {
		return "", false
	}
	return string(data[start+32 : start+32+length.Uint64()]), true
}
