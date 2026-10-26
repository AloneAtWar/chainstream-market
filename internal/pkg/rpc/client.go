// Package rpc is a minimal Ethereum JSON-RPC client for the calls the
// indexers make.
package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/metrics"
)

// balanceOfSelector is the ERC-20 balanceOf(address) selector.
const balanceOfSelector = "0x70a08231"

type Options struct {
	URL string
	// Chain labels the client's metrics.
	Chain string
	// BatchSize caps the calls sent in one JSON-RPC batch; zero uses 100.
	BatchSize int
	// Timeout bounds each HTTP request; zero uses 30 seconds.
	Timeout time.Duration
}

type Client struct {
	url       string
	chain     string
	batchSize int
	http      *http.Client
}

func New(options Options) (*Client, error) {
	if strings.TrimSpace(options.URL) == "" {
		return nil, errors.New("rpc: url is required")
	}
	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{url: options.URL, chain: options.Chain, batchSize: batchSize, http: &http.Client{Timeout: timeout}}, nil
}

// BalanceQuery asks for holder's balance of token.
type BalanceQuery struct {
	Token  string
	Holder string
}

// BalancesAt returns balanceOf for each query in the state after the block
// with blockHash. Pinning the block by hash makes the answer fail rather than
// silently come from another fork after a reorg. A nil balance means the
// call itself failed on-chain: the token reverted, has no code, or returned
// something that is not a uint256. Any other failure, such as a node without
// the block's state, is returned as an error.
func (client *Client) BalancesAt(ctx context.Context, blockHash string, queries []BalanceQuery) ([]*big.Int, error) {
	balances := make([]*big.Int, len(queries))
	for start := 0; start < len(queries); start += client.batchSize {
		end := min(start+client.batchSize, len(queries))
		if err := client.balanceBatch(ctx, blockHash, queries[start:end], balances[start:end]); err != nil {
			return nil, err
		}
	}
	return balances, nil
}

type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type response struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (err *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", err.Code, err.Message) }

// reverted reports whether the error is the contract failing, as opposed to
// the node failing.
func (err *rpcError) reverted() bool {
	return err.Code == 3 || strings.Contains(strings.ToLower(err.Message), "revert")
}

func (client *Client) balanceBatch(ctx context.Context, blockHash string, queries []BalanceQuery, out []*big.Int) error {
	block := map[string]any{"blockHash": blockHash, "requireCanonical": false}
	requests := make([]request, len(queries))
	for index, query := range queries {
		holder := strings.TrimPrefix(strings.ToLower(query.Holder), "0x")
		if len(holder) != 40 {
			return fmt.Errorf("rpc: invalid holder address %q", query.Holder)
		}
		call := map[string]string{"to": query.Token, "data": balanceOfSelector + strings.Repeat("0", 24) + holder}
		requests[index] = request{JSONRPC: "2.0", ID: index, Method: "eth_call", Params: []any{call, block}}
	}
	responses, err := client.post(ctx, requests)
	if err != nil {
		return err
	}
	seen := make([]bool, len(queries))
	for _, reply := range responses {
		if reply.ID < 0 || reply.ID >= len(queries) || seen[reply.ID] {
			return fmt.Errorf("rpc: unexpected response id %d", reply.ID)
		}
		seen[reply.ID] = true
		if reply.Error != nil {
			if reply.Error.reverted() {
				continue
			}
			metrics.RPCFailures.WithLabelValues(client.chain).Inc()
			return fmt.Errorf("rpc: balanceOf %s at %s: %w", queries[reply.ID].Token, blockHash, reply.Error)
		}
		out[reply.ID] = decodeUint256(reply.Result)
	}
	for index, ok := range seen {
		if !ok {
			return fmt.Errorf("rpc: no response for call %d", index)
		}
	}
	return nil
}

// decodeUint256 parses the first word of an eth_call result, or returns nil
// when there is none.
func decodeUint256(raw json.RawMessage) *big.Int {
	var hex string
	if err := json.Unmarshal(raw, &hex); err != nil {
		return nil
	}
	hex = strings.TrimPrefix(hex, "0x")
	if len(hex) < 64 {
		return nil
	}
	value, ok := new(big.Int).SetString(hex[:64], 16)
	if !ok {
		return nil
	}
	return value
}

// post sends one batch and records its latency and result.
func (client *Client) post(ctx context.Context, requests []request) ([]response, error) {
	start := time.Now()
	responses, err := client.send(ctx, requests)
	metrics.RPCSeconds.WithLabelValues(client.chain).Observe(time.Since(start).Seconds())
	result := "ok"
	if err != nil {
		result = "error"
		metrics.RPCFailures.WithLabelValues(client.chain).Inc()
	}
	metrics.RPCRequests.WithLabelValues(client.chain, result).Inc()
	return responses, err
}

func (client *Client) send(ctx context.Context, requests []request) ([]response, error) {
	body, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("rpc: encode: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, client.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("rpc: request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := client.http.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("rpc: post: %w", err)
	}
	defer httpResponse.Body.Close() //nolint:errcheck // read-only body
	raw, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, fmt.Errorf("rpc: read: %w", err)
	}
	if httpResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc: http %d: %s", httpResponse.StatusCode, truncate(raw))
	}
	var responses []response
	if err := json.Unmarshal(raw, &responses); err != nil {
		// A node rejects a whole batch with a single error object.
		var single response
		if json.Unmarshal(raw, &single) == nil && single.Error != nil {
			return nil, fmt.Errorf("rpc: batch: %w", single.Error)
		}
		return nil, fmt.Errorf("rpc: decode: %w: %s", err, truncate(raw))
	}
	return responses, nil
}

func truncate(raw []byte) string {
	if len(raw) > 200 {
		return string(raw[:200]) + "..."
	}
	return string(raw)
}

// Call is one eth_call: data sent to the contract at To.
type Call struct {
	To   string
	Data string
}

// CallLatest runs calls against the latest block and returns each call's
// return data. A nil result means the call itself failed on-chain: it
// reverted, or the address has no code. Any other failure is an error.
func (client *Client) CallLatest(ctx context.Context, calls []Call) ([][]byte, error) {
	results := make([][]byte, len(calls))
	for start := 0; start < len(calls); start += client.batchSize {
		end := min(start+client.batchSize, len(calls))
		if err := client.callBatch(ctx, calls[start:end], results[start:end]); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (client *Client) callBatch(ctx context.Context, calls []Call, out [][]byte) error {
	requests := make([]request, len(calls))
	for index, call := range calls {
		target := map[string]string{"to": call.To, "data": call.Data}
		requests[index] = request{JSONRPC: "2.0", ID: index, Method: "eth_call", Params: []any{target, "latest"}}
	}
	responses, err := client.post(ctx, requests)
	if err != nil {
		return err
	}
	seen := make([]bool, len(calls))
	for _, reply := range responses {
		if reply.ID < 0 || reply.ID >= len(calls) || seen[reply.ID] {
			return fmt.Errorf("rpc: unexpected response id %d", reply.ID)
		}
		seen[reply.ID] = true
		if reply.Error != nil {
			if reply.Error.reverted() {
				continue
			}
			metrics.RPCFailures.WithLabelValues(client.chain).Inc()
			return fmt.Errorf("rpc: call %s: %w", calls[reply.ID].To, reply.Error)
		}
		var data string
		if err := json.Unmarshal(reply.Result, &data); err != nil {
			return fmt.Errorf("rpc: call %s: %w", calls[reply.ID].To, err)
		}
		raw, err := hex.DecodeString(strings.TrimPrefix(data, "0x"))
		if err != nil {
			return fmt.Errorf("rpc: call %s: %w", calls[reply.ID].To, err)
		}
		// An address without code answers with empty data.
		if len(raw) > 0 {
			out[reply.ID] = raw
		}
	}
	for index, ok := range seen {
		if !ok {
			return fmt.Errorf("rpc: no response for call %d", index)
		}
	}
	return nil
}
