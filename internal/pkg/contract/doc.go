// Package contract holds the contract ABIs the services call, copied into
// the repository, and the abigen bindings generated from them, one package
// per contract. Calldata is packed and return data unpacked only through
// these bindings; internal/pkg/rpc sends the calls through
// oh-my-blockchain's EVM SDK.
//
// Each binding package has a *_generate.go file whose go:generate line runs
// abigen at the go-ethereum version go.mod requires. Run make abi (or
// go generate ./internal/pkg/contract/...) after changing an ABI or that
// version, and never edit a generated file by hand. CI regenerates the
// bindings and fails on any difference.
package contract
