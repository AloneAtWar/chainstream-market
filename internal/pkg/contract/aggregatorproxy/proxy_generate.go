package aggregatorproxy

// The abigen version must match github.com/ethereum/go-ethereum in go.mod;
// contract.TestAbigenMatchesGoMod fails when they drift.
//go:generate go run github.com/ethereum/go-ethereum/cmd/abigen@v1.17.4 --v2 --abi EACAggregatorProxy.abi --pkg aggregatorproxy --type Proxy --out proxy.go
