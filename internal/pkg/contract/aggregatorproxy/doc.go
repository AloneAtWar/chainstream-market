// Package aggregatorproxy holds abigen bindings for Chainlink price feed
// proxies.
//
// EACAggregatorProxy.abi is copied unchanged from
// @chainlink/contracts@0.8.0 abi/v0.6/EACAggregatorProxy.json, the proxy
// deployed in front of Chainlink's data feeds; later releases no longer ship
// it. A plain AggregatorProxy answers the same calls except phaseId and
// phaseAggregators. proxy.go is generated from it; run make abi (or
// go generate) after changing the ABI or the go-ethereum version, never edit
// it by hand.
package aggregatorproxy
