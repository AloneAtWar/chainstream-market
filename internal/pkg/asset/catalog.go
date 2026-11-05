// Package asset holds what every component knows about token addresses: the
// per-chain Catalog of USD anchors, quote preferences and aliases, and the
// metadata chainstream-evm publishes for each asset.
//
// asset 包是所有组件共享的代币地址知识：每条链的 Catalog（USD 锚定资产、计价优先级、
// 别名，例如原生币伪地址映射到 wrapped 代币），以及 chainstream-evm 发布的资产元数据解析。
package asset

import (
	"strings"

	"github.com/shopspring/decimal"
)

// NativeAddress is the pseudo-address chainstream-evm uses for the chain's
// native coin.
const NativeAddress = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

// Catalog holds a chain's USD anchors and quote preferences.
//
// Catalog 保存一条链的 USD 锚定资产（价格图的起点，没有预言机报价时按 $1）和
// 计价优先级（决定交易对中谁做 quote）。
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
		address = Normalize(address)
		if _, exists := catalog.priority[address]; !exists && address != "" {
			catalog.priority[address] = rank
			rank++
		}
	}
	for _, anchor := range spec.Anchors {
		catalog.anchors[Normalize(anchor)] = decimal.NewFromInt(1)
		add(anchor)
	}
	if wrapped := Normalize(spec.WrappedNative); wrapped != "" {
		add(wrapped)
		catalog.aliases[NativeAddress] = wrapped
	}
	for _, reference := range spec.References {
		add(reference)
	}
	return catalog
}

// DefaultCatalog returns the built-in catalog for ETH, BSC, Base and
// Robinhood Chain, and an empty catalog for other chains.
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
	case 4663:
		// Robinhood Chain: USDG is its dollar, and the stock tokens that
		// have Chainlink feeds are preferred quotes after WETH, since
		// launches such as Long.xyz pair against them.
		return NewCatalog(CatalogSpec{
			Anchors: []string{
				"0x5fc5360d0400a0fd4f2af552add042d716f1d168", // USDG
			},
			WrappedNative: "0x0bd7d308f8e1639fab988df18a8011f41eacad73", // WETH
			References: []string{
				"0xaf3d76f1834a1d425780943c99ea8a608f8a93f9", // AAPL
				"0x86923f96303d656e4aa86d9d42d1e57ad2023fdc", // AMD
				"0x12f190a9f9d7d37a250758b26824b97ce941bf54", // AMZN
				"0x47f93d52cbec7c6d2cfc080e154002370a60daea", // ASML
				"0xad25ac6c84d497db898fa1e8387bf6af3532a1c4", // BABA
				"0xcbb95bbf36099d34da091dc6fa6f49efa257cee3", // CLSK
				"0x6330d8c3178a418788df01a47479c0ce7ccf450b", // COIN
				"0xdf0992e440dd0be65bd8439b609d6d4366bf1cb5", // CRCL
				"0x5f10a1c971b69e47e059e1dc91901b59b3fb49c3", // CRWV
				"0x941ae714ec6d8130c7b75d67160ca08f1e7d11dd", // DELL
				"0x7f0abef0c07280f82c6a08ead09ded6bae2c13fc", // EWY
				"0x1b0e319c6a659f002271b69db8a7df2f911c153e", // GME
				"0x2e0847e8910a9732eb3fb1bb4b70a580adad4fe3", // GOOGL
				"0xc72b96e0e48ecd4dc75e1e45396e26300bc39681", // INTC
				"0x558378e000d634a36593e338ebacdd6207640efe", // IONQ
				"0xc0d6457c16cc70d6790dd43521c899c87ce02f35", // META
				"0xe93237c50d904957cf27e7b1133b510c669c2e74", // MSFT
				"0xec262a75e413fafd0df80480274532c79d42da09", // MSTR
				"0xff080c8ce2e5feadaca0da81314ae59d232d4afd", // MU
				"0x9d9c6684f596f66a64c030b93a886d51fd4d7931", // NBIS
				"0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec", // NVDA
				"0xb0992820e760d836549ba69bc7598b4af75dee03", // ORCL
				"0x894e1ec2d74ffe5aef8dc8a9e84686accb964f2a", // PLTR
				"0xd5f3879160bc7c32ebb4dc785f8a4f505888de68", // QQQ
				"0x284358abc07f9359f19f4b5b4ac91901be2597ba", // RGTI
				"0x3b14c39e89d60d627b42a1a4ca45b5bb45fc12e2", // RKLB
				"0x92fd66527192e3e61d4ddd13322aa222de86f9b5", // SGOV
				"0x411efb0e7f985935daec3d4c3ebaea0d0ad7d89f", // SLV
				"0xb90a19ff0af67f7779aff50a882a9cff42446400", // SNDK
				"0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea", // SPCX
				"0x117cc2133c37b721f49de2a7a74833232b3b4c0c", // SPY
				"0x322f0929c4625ed5bad873c95208d54e1c003b2d", // TSLA
				"0x58ffe4a942d3885baa22d7520691f611ef09e7aa", // TSM
				"0xd917b029c761d264c6a312bbbcda868658ef86a6", // USAR
				"0xa30fa36db767ad9ed3f7a60fc79526fb4d56d344", // USO
			},
		})
	default:
		return NewCatalog(CatalogSpec{})
	}
}

// Canonical maps aliases, such as the native pseudo-address, to the token
// that carries their price.
func (catalog Catalog) Canonical(token string) string {
	token = Normalize(token)
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

// Normalize lowercases address and trims its spaces.
func Normalize(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}
