// Package chainlink reads Chainlink price feed proxies, to find the
// aggregators whose AnswerUpdated logs carry a feed's answers.
package chainlink

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/contract/aggregatorproxy"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
)

// maxPhases bounds how many phases are read from one proxy.
const maxPhases = 1000

// maxDecimals bounds the decimals a feed may report.
const maxDecimals = 36

var proxyABI = aggregatorproxy.NewProxy()

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
		{To: address, Data: proxyABI.PackDecimals()},
		{To: address, Data: proxyABI.PackDescription()},
		{To: address, Data: proxyABI.PackAggregator()},
		{To: address, Data: proxyABI.PackPhaseId()},
	})
	if err != nil {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s: %w", address, err)
	}
	decimals, err := proxyABI.UnpackDecimals(results[0])
	if err != nil || decimals > maxDecimals {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no decimals(); is it a feed proxy?", address)
	}
	description, err := proxyABI.UnpackDescription(results[1])
	if err != nil {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no description()", address)
	}
	current, err := proxyABI.UnpackAggregator(results[2])
	if err != nil || current == (common.Address{}) {
		return Proxy{}, fmt.Errorf("chainlink: proxy %s has no aggregator()", address)
	}
	proxy := Proxy{Address: address, Description: description, Decimals: decimals}

	// A plain AggregatorProxy has no phaseId(); its call fails and only the
	// current aggregator is listed.
	if phase, err := proxyABI.UnpackPhaseId(results[3]); err == nil && phase > 0 {
		if phase > maxPhases {
			return Proxy{}, fmt.Errorf("chainlink: proxy %s reports phase %d", address, phase)
		}
		calls := make([]rpc.Call, phase)
		for index := range calls {
			calls[index] = rpc.Call{To: address, Data: proxyABI.PackPhaseAggregators(uint16(index + 1))}
		}
		phases, err := caller.CallLatest(ctx, calls)
		if err != nil {
			return Proxy{}, fmt.Errorf("chainlink: proxy %s phases: %w", address, err)
		}
		for _, result := range phases {
			if aggregator, err := proxyABI.UnpackPhaseAggregators(result); err == nil && aggregator != (common.Address{}) {
				proxy.add(lower(aggregator))
			}
		}
	}
	proxy.add(lower(current))
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

func lower(address common.Address) string { return strings.ToLower(address.Hex()) }
