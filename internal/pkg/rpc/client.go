// Package rpc reads contract state for the indexers through
// oh-my-blockchain's EVM SDK. Calldata is packed and return data unpacked
// with the abigen bindings in internal/pkg/contract; this package only adds
// the indexers' failure rules and metrics on top of the SDK's batching,
// retries and multicall aggregation.
//
// rpc 包通过 oh-my-blockchain 的 EVM SDK 读取合约状态（批量、重试、multicall 聚合）：
// 余额按区块哈希锁定（EIP-1898，exact）或在不早于当前块的 latest 读取并报告实际块高
// （latest），以及最新区块上的调用，并加上索引器的失败规则与指标。
// calldata 一律用 abigen 绑定编解码，不手写 selector。TRON 的 base58 地址在打包前转成 hex。
package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/AloneAtWar/oh-my-blockchain/chain/evm"
	evmsdk "github.com/AloneAtWar/oh-my-blockchain/chain/evm/sdk"
	evmsdkimpl "github.com/AloneAtWar/oh-my-blockchain/chain/evm/sdk/impl"
	"github.com/AloneAtWar/oh-my-blockchain/chain/evm/sdk/multicall"
	"github.com/ethereum/go-ethereum/common"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/contract/erc20"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
)

type Options struct {
	URL string
	// ChainID selects the SDK's chain dialect and multicall contract. Zero
	// is a plain EVM node: every call is its own eth_call.
	ChainID uint64
	// Chain labels the client's metrics.
	Chain string
	// BatchSize caps the eth_calls sent in one JSON-RPC batch; zero keeps
	// the SDK's default.
	BatchSize int
	// Timeout bounds each HTTP request; zero uses 30 seconds.
	Timeout time.Duration
	// Logger receives the SDK's logs; nil uses slog.Default.
	Logger *slog.Logger
	// NativeBalanceContract answers getEthBalance(address) for native
	// balances. Empty uses the chain's Multicall3, or failing that its
	// Uniswap interface multicall; a chain with neither has no native
	// balances.
	NativeBalanceContract string
	// Exact reads balances at the applied block's hash. Otherwise they are
	// read at the latest block no older than it, which needs no historical
	// state. TRON only supports latest.
	Exact bool
}

type Client struct {
	chain  string
	sdk    evmsdk.ContractCaller
	native string
	exact  bool
	tron   bool
}

var tokenABI = erc20.NewToken()

func New(options Options) (*Client, error) {
	if strings.TrimSpace(options.URL) == "" {
		return nil, errors.New("rpc: url is required")
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sdk, err := evmsdkimpl.NewEvmSdk(evm.ChainType(options.ChainID), options.URL,
		evmsdkimpl.WithHTTPClient(&http.Client{Timeout: timeout}),
		evmsdkimpl.WithLogger(kratosLogger{logger: logger.With("component", "evmsdk")}),
		evmsdkimpl.WithMiddlewares(requestMetrics(options.Chain)),
		evmsdkimpl.WithMethodBatching(evmsdkimpl.OpCallContract, options.BatchSize, 0),
	)
	if err != nil {
		return nil, fmt.Errorf("rpc: %w", err)
	}
	native := strings.ToLower(strings.TrimSpace(options.NativeBalanceContract))
	if native == "" {
		native = strings.ToLower(multicall.NativeBalanceAddress(evm.ChainType(options.ChainID)))
	}
	tron := evm.ChainType(options.ChainID) == evm.TRON
	if tron && options.Exact {
		return nil, errors.New("rpc: TRON nodes only serve the latest block, exact balance reads are unsupported")
	}
	return &Client{chain: options.Chain, sdk: sdk, native: native, exact: options.Exact, tron: tron}, nil
}

// NativeBalances reports whether BalancesAt can read native balances.
func (client *Client) NativeBalances() bool { return client.native != "" }

// BalanceQuery asks for holder's balance of token. Token NativeToken asks
// for the native coin balance.
type BalanceQuery struct {
	Token  string
	Holder string
}

// NativeToken is the pseudo-address of the chain's native coin.
const NativeToken = asset.NativeAddress

// Block is the block a balance read is for: the indexer's block being
// applied.
//
// Block 是余额读取所针对的区块，即索引器正在应用的块。
type Block struct {
	Height int64
	Hash   string
}

// Balance is one balance and the block whose post-state it is. Amount is
// nil when the call itself failed on-chain: the token reverted, has no
// code, or returned something that is not a uint256.
//
// Balance 是一次余额读取的结果和它所属的块高（余额是该块执行后的状态）；调用在链上失败时 Amount 为 nil。
type Balance struct {
	Amount *big.Int
	Height int64
}

// Balances returns balanceOf for each query. Exact clients read the state
// after block, pinned by its hash, so a reorg makes the answer fail rather
// than silently come from another fork; every Height is block.Height.
// Latest clients read the latest block no older than block and report the
// block each answer was executed at, which needs no historical state.
// Native balances are read with the multicall contract's getEthBalance, so
// they batch with balanceOf. Any failure other than an on-chain one, such as
// a node without the block's state, is returned as an error, and so is a
// native query on a chain without native balances.
//
// Balances 读取每个查询的 balanceOf：exact 按区块哈希锁定，Height 都是 block.Height；
// latest 在不早于 block 的 latest 读取并报告每个结果实际执行的块高，不需要历史状态。
func (client *Client) Balances(ctx context.Context, block Block, queries []BalanceQuery) ([]Balance, error) {
	requests := make([]evmsdk.RPCCallReq, len(queries))
	for index, query := range queries {
		holder, err := client.hexAddress(query.Holder)
		if err != nil {
			return nil, fmt.Errorf("rpc: invalid holder address %q: %w", query.Holder, err)
		}
		requests[index] = evmsdk.RPCCallReq{
			Address:  query.Token,
			Calldata: common.Bytes2Hex(tokenABI.PackBalanceOf(holder)),
		}
		if query.Token == NativeToken {
			if client.native == "" {
				return nil, errors.New("rpc: chain has no multicall contract to read native balances")
			}
			calldata, err := multicall.UniswapInterfaceABI.Pack(multicall.MethodGetEthBalance, holder)
			if err != nil {
				return nil, fmt.Errorf("rpc: pack getEthBalance: %w", err)
			}
			requests[index].Address, requests[index].Calldata = client.native, common.Bytes2Hex(calldata)
		}
		if client.exact {
			requests[index].BlockHash = block.Hash
		} else {
			requests[index].Read = evmsdk.StateRead{
				Requirement: evmsdk.ReadAtLeast, QueryAt: evmsdk.LatestBlockNumber, MinimumHeight: block.Height,
			}
		}
	}
	results, heights, err := client.callAt(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("rpc: balanceOf at %d %s: %w", block.Height, block.Hash, err)
	}
	balances := make([]Balance, len(results))
	for index, result := range results {
		balances[index].Height = block.Height
		if !client.exact {
			balances[index].Height = heights[index]
		}
		if balance, err := tokenABI.UnpackBalanceOf(result); err == nil {
			balances[index].Amount = balance
		}
	}
	return balances, nil
}

// hexAddress parses a holder: a 0x address, or on TRON a base58 one, which
// the ABI encodes by its 20-byte hex form.
func (client *Client) hexAddress(address string) (common.Address, error) {
	if common.IsHexAddress(address) {
		return common.HexToAddress(address), nil
	}
	if client.tron {
		return tronHexAddress(address)
	}
	return common.Address{}, errors.New("not a hex address")
}

// Call is one eth_call: data sent to the contract at To.
type Call struct {
	To   string
	Data []byte
}

// CallLatest runs calls against the latest block and returns each call's
// return data. A nil result means the call itself failed on-chain: it
// reverted, or the address has no code. Any other failure is an error.
func (client *Client) CallLatest(ctx context.Context, calls []Call) ([][]byte, error) {
	requests := make([]evmsdk.RPCCallReq, len(calls))
	for index, call := range calls {
		requests[index] = evmsdk.RPCCallReq{
			Address: call.To, Calldata: common.Bytes2Hex(call.Data), BlockHeight: evmsdk.LatestBlockNumber,
		}
	}
	results, _, err := client.callAt(ctx, requests)
	return results, err
}

// callAt runs requests and applies the failure rules: a revert or empty
// return data is a nil result, any other node error fails the whole call.
// It also returns the block each request executed at, or -1 when the
// request named no state read; a state read without one is an error.
func (client *Client) callAt(ctx context.Context, requests []evmsdk.RPCCallReq) ([][]byte, []int64, error) {
	if len(requests) == 0 {
		return nil, nil, nil
	}
	responses, err := client.sdk.CallContract(ctx, requests)
	if err != nil {
		return nil, nil, err
	}
	if len(responses) != len(requests) {
		return nil, nil, fmt.Errorf("%d responses for %d calls", len(responses), len(requests))
	}
	results := make([][]byte, len(responses))
	heights := make([]int64, len(responses))
	for index, response := range responses {
		heights[index] = -1
		if !requests[index].Read.IsZero() {
			execution := response.Execution
			if execution == nil || execution.BlockNumber == nil || !execution.BlockNumber.IsInt64() {
				return nil, nil, fmt.Errorf("call %s: no execution height for a latest read", requests[index].Address)
			}
			heights[index] = execution.BlockNumber.Int64()
		}
		if !response.Success {
			if response.Err != nil && !evmsdk.IsExecutionReverted(response.Err) {
				metrics.RPCFailures.WithLabelValues(client.chain).Inc()
				return nil, nil, fmt.Errorf("call %s: %w", requests[index].Address, response.Err)
			}
			continue
		}
		// An address without code answers with empty data.
		if len(response.ReturnData) > 0 {
			results[index] = response.ReturnData
		}
	}
	return results, heights, nil
}
