package market

import (
	"strings"

	"github.com/shopspring/decimal"
)

// NativeAddress is the pseudo-address chainstream-evm uses for the chain's
// native coin.
const NativeAddress = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

// Catalog holds a chain's USD anchors and quote preferences.
type Catalog struct {
	anchors  map[string]decimal.Decimal
	priority map[string]int
	aliases  map[string]string
}

// CatalogSpec lists a chain's market assets in quote-priority order.
type CatalogSpec struct {
	// Anchors are USD-pegged assets, strongest quote first. Until oracle
	// updates arrive they are valued at $1.
	Anchors []string
	// WrappedNative is priced through the graph; the native pseudo-address
	// is an alias of it.
	WrappedNative string
	// References are preferred quote assets after the native coin, such as
	// BTC wrappers. They are priced through the graph.
	References []string
}

func NewCatalog(spec CatalogSpec) Catalog {
	catalog := Catalog{
		anchors:  make(map[string]decimal.Decimal),
		priority: make(map[string]int),
		aliases:  make(map[string]string),
	}
	rank := 0
	add := func(address string) {
		address = normalize(address)
		if _, exists := catalog.priority[address]; !exists && address != "" {
			catalog.priority[address] = rank
			rank++
		}
	}
	for _, anchor := range spec.Anchors {
		catalog.anchors[normalize(anchor)] = decimal.NewFromInt(1)
		add(anchor)
	}
	if wrapped := normalize(spec.WrappedNative); wrapped != "" {
		add(wrapped)
		catalog.aliases[NativeAddress] = wrapped
	}
	for _, reference := range spec.References {
		add(reference)
	}
	return catalog
}

// DefaultCatalog returns the built-in catalog for ETH, BSC and Base, and an
// empty catalog for other chains.
func DefaultCatalog(chainID uint64) Catalog {
	switch chainID {
	case 1:
		return NewCatalog(CatalogSpec{
			Anchors: []string{
				"0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
				"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
				"0xdc035d45d973e3ec169d2276ddab16f1e407384f", // USDS
				"0x4c9edd5852cd905f086c759e8383e09bff1e68b3", // USDe
				"0x6b175474e89094c44da98b954eedeac495271d0f", // DAI
			},
			WrappedNative: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // WETH
			References: []string{
				"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", // WBTC
				"0xae7ab96520de3a18e5e111b5eaab095312d7fe84", // stETH
			},
		})
	case 56:
		return NewCatalog(CatalogSpec{
			Anchors: []string{
				"0x55d398326f99059ff775485246999027b3197955", // USDT
				"0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", // USDC
				"0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", // USD1
				"0xe9e7cea3dedca5984780bafc599bd69add087d56", // BUSD
				"0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3", // DAI
			},
			WrappedNative: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", // WBNB
			References: []string{
				"0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", // BTCB
				"0x2170ed0880ac9a755fd29b2688956bd959f933f8", // ETH
			},
		})
	case 8453:
		return NewCatalog(CatalogSpec{
			Anchors: []string{
				"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", // USDC
				"0xfde4c96c8593536e31f229ea8f37b2ada2699bb2", // USDT
				"0x50c5725949a6f0c72e6c4a641f24049a917db0cb", // DAI
				"0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", // USDbC
			},
			WrappedNative: "0x4200000000000000000000000000000000000006", // WETH
			References: []string{
				"0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf", // cbBTC
			},
		})
	default:
		return NewCatalog(CatalogSpec{})
	}
}

// Canonical maps aliases, such as the native pseudo-address, to the token
// that carries their price.
func (catalog Catalog) Canonical(token string) string {
	token = normalize(token)
	if alias, ok := catalog.aliases[token]; ok {
		return alias
	}
	return token
}

func (catalog Catalog) Anchor(token string) (decimal.Decimal, bool) {
	price, ok := catalog.anchors[catalog.Canonical(token)]
	return price, ok
}

// QuotePriority returns a lower-is-stronger rank for preferred quote assets.
func (catalog Catalog) QuotePriority(token string) (int, bool) {
	rank, ok := catalog.priority[catalog.Canonical(token)]
	return rank, ok
}

func normalize(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}
