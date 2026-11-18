// Package rpc reads contract state for the indexers through
// oh-my-blockchain's EVM SDK. Calldata is packed and return data unpacked
// with the abigen bindings in internal/pkg/contract; this package only adds
// the indexers' failure rules and metrics on top of the SDK's batching,
// retries and multicall aggregation.
//
// rpc 包通过 oh-my-blockchain 的 EVM SDK 读取合约状态（批量、重试、multicall 聚合）：
// 余额按区块哈希锁定（EIP-1898，exact），或在解析不早于当前块的链头后按其哈希锁定
// （普通 EVM latest）；TRON latest 校验调用前后链头高度和哈希。返回值包含实际执行块证据。
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
// Balance 是一次余额读取的结果和它所属的区块（余额是该块执行后的状态）；调用在链上失败时 Amount 为 nil。
type Balance struct {
	Amount *big.Int
	Height int64
	Hash   string
}

const (
	latestReadAttempts   = 3
	latestReadRetryDelay = 300 * time.Millisecond
)

// Balances returns balanceOf for each query and the execution block's height
// and hash. Exact clients read at block's hash. Ordinary EVM latest clients
// resolve a head no older than block and pin calls to that head's hash. TRON
// cannot pin calls by hash, so it cross-checks reported execution heights and
// accepts a latest read only when the head's height and hash are unchanged
// before and after the calls. Pinning does not
// make an unfinalized block final; reverts are still handled by the indexer.
// Native balances use the multicall contract's getEthBalance and batch with
// balanceOf. Node failures are returned, while an on-chain failure gives a nil
// Amount.
//
// Balances 读取每个查询的 balanceOf，并返回实际执行块的高度和哈希。普通 EVM 的
// latest 先解析不早于当前块的链头再按哈希锁定；链头恰好等于当前块时必须同哈希。
// TRON 交叉检查执行高度，并校验调用前后高度和哈希不变。
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
	}
	if len(requests) == 0 {
		return []Balance{}, nil
	}
	if client.tron && !client.exact {
		results, read, err := client.callTronLatest(ctx, block, requests)
		if err != nil {
			return nil, fmt.Errorf("rpc: balanceOf at %d %s: %w", block.Height, block.Hash, err)
		}
		return decodeBalances(results, read), nil
	}
	read := Block{Height: block.Height, Hash: strings.ToLower(block.Hash)}
	if !client.exact {
		head, err := client.latestHead(ctx, block)
		if err != nil {
			return nil, fmt.Errorf("rpc: balanceOf at %d %s: %w", block.Height, block.Hash, err)
		}
		read = Block{Height: head.Number, Hash: head.Hash.Hex()}
	}
	for index := range requests {
		requests[index].BlockHash = read.Hash
	}
	results, _, err := client.callAt(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("rpc: balanceOf at %d %s: %w", block.Height, block.Hash, err)
	}
	return decodeBalances(results, read), nil
}

func decodeBalances(results [][]byte, block Block) []Balance {
	balances := make([]Balance, len(results))
	for index, result := range results {
		balances[index].Height, balances[index].Hash = block.Height, block.Hash
		if balance, err := tokenABI.UnpackBalanceOf(result); err == nil {
			balances[index].Amount = balance
		}
	}
	return balances
}

func (client *Client) latestHead(ctx context.Context, block Block) (evmsdk.BlockHead, error) {
	reader, ok := client.sdk.(evmsdk.BlockHeadReader)
	if !ok {
		return evmsdk.BlockHead{}, fmt.Errorf("rpc: %T cannot read block heads", client.sdk)
	}
	for attempt := 0; ; attempt++ {
		head, err := reader.GetBlockHeadByTag(ctx, evmsdk.LatestBlockNumber)
		if err != nil {
			return evmsdk.BlockHead{}, err
		}
		if head.Hash == (common.Hash{}) {
			return evmsdk.BlockHead{}, errors.New("rpc: latest block has no hash")
		}
		if head.Number > block.Height ||
			(head.Number == block.Height && strings.EqualFold(head.Hash.Hex(), block.Hash)) {
			return head, nil
		}
		var unstable error
		if head.Number < block.Height {
			unstable = fmt.Errorf("%w: latest block %d precedes minimum %d",
				evmsdk.ErrStateReadUnstable, head.Number, block.Height)
		} else {
			unstable = fmt.Errorf("%w: latest block %d hash %s does not match applied hash %s",
				evmsdk.ErrStateReadUnstable, head.Number, head.Hash, block.Hash)
		}
		if attempt == latestReadAttempts-1 {
			return evmsdk.BlockHead{}, unstable
		}
		timer := time.NewTimer(latestReadRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return evmsdk.BlockHead{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (client *Client) callTronLatest(ctx context.Context, block Block, requests []evmsdk.RPCCallReq) (
	[][]byte, Block, error,
) {
	var unstable error
	for range latestReadAttempts {
		before, err := client.latestHead(ctx, block)
		if err != nil {
			return nil, Block{}, err
		}
		for index := range requests {
			requests[index].BlockHeight = evmsdk.LatestBlockNumber
		}
		results, heights, err := client.callAt(ctx, requests)
		if err != nil {
			return nil, Block{}, err
		}
		mismatched := false
		for index, height := range heights {
			if height >= 0 && height != before.Number {
				unstable = fmt.Errorf("%w: call %d executed at %d, head was %d %s", evmsdk.ErrStateReadUnstable,
					index, height, before.Number, before.Hash)
				mismatched = true
				break
			}
		}
		if mismatched {
			continue
		}
		after, err := client.latestHead(ctx, block)
		if err != nil {
			return nil, Block{}, err
		}
		if before != after {
			unstable = fmt.Errorf("%w: latest changed from %d %s to %d %s", evmsdk.ErrStateReadUnstable,
				before.Number, before.Hash, after.Number, after.Hash)
			continue
		}
		return results, Block{Height: before.Number, Hash: before.Hash.Hex()}, nil
	}
	return nil, Block{}, unstable
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
// It returns execution heights whenever the mechanism reports them, or -1
// when unavailable; a configured state read requires that evidence.
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
		if execution := response.Execution; execution != nil && execution.BlockNumber != nil {
			if !execution.BlockNumber.IsInt64() || execution.BlockNumber.Sign() < 0 {
				return nil, nil, fmt.Errorf("call %s: invalid execution height", requests[index].Address)
			}
			heights[index] = execution.BlockNumber.Int64()
		}
		if !requests[index].Read.IsZero() && heights[index] < 0 {
			return nil, nil, fmt.Errorf("call %s: no execution height for a latest read", requests[index].Address)
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
