// Package rpc reads contract state for the indexers through
// oh-my-blockchain's EVM SDK. Calldata is packed and return data unpacked
// with the abigen bindings in internal/pkg/contract; this package only adds
// the indexers' failure rules and metrics on top of the SDK's batching,
// retries and multicall aggregation.
//
// rpc 包通过 oh-my-blockchain 的 EVM SDK 读取合约状态（批量、重试、multicall 聚合）：
// 锁定区块哈希的 balanceOf（EIP-1898）、最新区块上的调用，并加上索引器的失败规则与指标。
// calldata 一律用 abigen 绑定编解码，不手写 selector。
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
}

type Client struct {
	chain  string
	sdk    evmsdk.ContractCaller
	native string
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
	return &Client{chain: options.Chain, sdk: sdk, native: native}, nil
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

// BalancesAt returns balanceOf for each query in the state after the block
// with blockHash. Pinning the block by hash makes the answer fail rather than
// silently come from another fork after a reorg. Native balances are read
// with the multicall contract's getEthBalance, so they batch with balanceOf.
// A nil balance means the call itself failed on-chain: the token reverted,
// has no code, or returned something that is not a uint256. Any other
// failure, such as a node without the block's state, is returned as an
// error, and so is a native query on a chain without native balances.
func (client *Client) BalancesAt(ctx context.Context, blockHash string, queries []BalanceQuery) ([]*big.Int, error) {
	requests := make([]evmsdk.RPCCallReq, len(queries))
	for index, query := range queries {
		if !common.IsHexAddress(query.Holder) {
			return nil, fmt.Errorf("rpc: invalid holder address %q", query.Holder)
		}
		requests[index] = evmsdk.RPCCallReq{
			Address:   query.Token,
			Calldata:  common.Bytes2Hex(tokenABI.PackBalanceOf(common.HexToAddress(query.Holder))),
			BlockHash: blockHash,
		}
		if query.Token == NativeToken {
			if client.native == "" {
				return nil, errors.New("rpc: chain has no multicall contract to read native balances")
			}
			calldata, err := multicall.UniswapInterfaceABI.Pack(multicall.MethodGetEthBalance, common.HexToAddress(query.Holder))
			if err != nil {
				return nil, fmt.Errorf("rpc: pack getEthBalance: %w", err)
			}
			requests[index].Address, requests[index].Calldata = client.native, common.Bytes2Hex(calldata)
		}
	}
	results, err := client.call(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("rpc: balanceOf at %s: %w", blockHash, err)
	}
	balances := make([]*big.Int, len(results))
	for index, result := range results {
		if balance, err := tokenABI.UnpackBalanceOf(result); err == nil {
			balances[index] = balance
		}
	}
	return balances, nil
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
	return client.call(ctx, requests)
}

// call runs requests and applies the failure rules: a revert or empty
// return data is a nil result, any other node error fails the whole call.
func (client *Client) call(ctx context.Context, requests []evmsdk.RPCCallReq) ([][]byte, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	responses, err := client.sdk.CallContract(ctx, requests)
	if err != nil {
		return nil, err
	}
	if len(responses) != len(requests) {
		return nil, fmt.Errorf("%d responses for %d calls", len(responses), len(requests))
	}
	results := make([][]byte, len(responses))
	for index, response := range responses {
		if !response.Success {
			if response.Err != nil && !evmsdk.IsExecutionReverted(response.Err) {
				metrics.RPCFailures.WithLabelValues(client.chain).Inc()
				return nil, fmt.Errorf("call %s: %w", requests[index].Address, response.Err)
			}
			continue
		}
		// An address without code answers with empty data.
		if len(response.ReturnData) > 0 {
			results[index] = response.ReturnData
		}
	}
	return results, nil
}
