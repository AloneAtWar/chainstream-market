package chainlink

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/rpc"
)

const (
	proxyAddress = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE"
	phaseOne     = "0x1111111111111111111111111111111111111111"
	phaseTwo     = "0x2222222222222222222222222222222222222222"
)

// fakeChain answers calls from a table of return data by target and data.
// Calls missing from the table revert.
type fakeChain map[string][]byte

func (chain fakeChain) CallLatest(_ context.Context, calls []rpc.Call) ([][]byte, error) {
	results := make([][]byte, len(calls))
	for index, call := range calls {
		results[index] = chain[strings.ToLower(call.To)+call.Data]
	}
	return results, nil
}

func uintWord(value uint64) []byte {
	raw, _ := hex.DecodeString(fmt.Sprintf("%064x", value)) //nolint:errcheck // well-formed
	return raw
}

func addressData(address string) []byte {
	raw, _ := hex.DecodeString(strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")) //nolint:errcheck // well-formed
	return raw
}

func stringData(value string) []byte {
	data := append(uintWord(32), uintWord(uint64(len(value)))...)
	padded := make([]byte, (len(value)+31)/32*32)
	copy(padded, value)
	return append(data, padded...)
}

func feedProxy(phases ...string) fakeChain {
	proxy := strings.ToLower(proxyAddress)
	return fakeChain{
		proxy + selectorDecimals:    uintWord(8),
		proxy + selectorDescription: stringData("BNB / USD"),
		proxy + selectorAggregator:  addressData(phases[len(phases)-1]),
	}
}

func TestResolveListsEveryPhaseAggregator(t *testing.T) {
	chain := feedProxy(phaseOne, phaseTwo)
	proxy := strings.ToLower(proxyAddress)
	chain[proxy+selectorPhaseID] = uintWord(3)
	chain[proxy+selectorPhaseAggregators+fmt.Sprintf("%064x", 1)] = addressData(phaseTwo)
	chain[proxy+selectorPhaseAggregators+fmt.Sprintf("%064x", 2)] = addressData(phaseOne)
	// Phase 3 was never set: the zero address is skipped.
	chain[proxy+selectorPhaseAggregators+fmt.Sprintf("%064x", 3)] = uintWord(0)

	resolved, err := Resolve(context.Background(), chain, proxyAddress)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	want := Proxy{Address: proxy, Description: "BNB / USD", Decimals: 8, Aggregators: []string{phaseOne, phaseTwo}}
	if !reflect.DeepEqual(resolved, want) {
		t.Fatalf("Resolve() = %+v, want %+v", resolved, want)
	}
}

func TestResolveWithoutPhasesUsesCurrentAggregator(t *testing.T) {
	resolved, err := Resolve(context.Background(), feedProxy(phaseOne), proxyAddress)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !reflect.DeepEqual(resolved.Aggregators, []string{phaseOne}) {
		t.Fatalf("aggregators = %v", resolved.Aggregators)
	}
}

func TestResolveRejectsNonProxies(t *testing.T) {
	if _, err := Resolve(context.Background(), fakeChain{}, proxyAddress); err == nil {
		t.Fatal("Resolve() of an address without code error = nil")
	}
	chain := feedProxy(phaseOne)
	chain[strings.ToLower(proxyAddress)+selectorDescription] = uintWord(7)
	if _, err := Resolve(context.Background(), chain, proxyAddress); err == nil {
		t.Fatal("Resolve() with a malformed description error = nil")
	}
}
