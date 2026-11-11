// Package rpctest is a fake Ethereum node that answers balanceOf calls.
package rpctest

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// Node answers eth_call balanceOf from a table of balances per block hash.
// Unknown balances read as zero. Calls to a reverting token fail on-chain;
// calls at a missing block fail like a node without that state. Calls by
// block number or the latest tag, and eth_getBlockByNumber, use the heights
// set with SetHead.
type Node struct {
	*httptest.Server
	mu        sync.Mutex
	balances  map[string]map[string]*big.Int
	reverting map[string]bool
	missing   map[string]bool
	hashes    map[int64]string
	head      int64
	calls     int
}

func NewNode() *Node {
	node := &Node{
		balances: make(map[string]map[string]*big.Int), reverting: make(map[string]bool), missing: make(map[string]bool),
		hashes: make(map[int64]string),
	}
	node.Server = httptest.NewServer(http.HandlerFunc(node.serve))
	return node
}

// SetBalance sets holder's balance of token after the block with blockHash.
func (node *Node) SetBalance(blockHash, token, holder string, balance int64) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.balances[blockHash] == nil {
		node.balances[blockHash] = make(map[string]*big.Int)
	}
	node.balances[blockHash][key(token, holder)] = big.NewInt(balance)
}

// SetHead makes the block with blockHash at height the latest block.
//
// SetHead 把 height 处哈希为 blockHash 的块设为最新块。
func (node *Node) SetHead(height int64, blockHash string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.hashes[height] = blockHash
	node.head = height
}

func (node *Node) Revert(token string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.reverting[strings.ToLower(token)] = true
}

func (node *Node) Missing(blockHash string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.missing[blockHash] = true
}

// Calls returns how many eth_call requests the node answered.
func (node *Node) Calls() int {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.calls
}

func key(token, holder string) string { return strings.ToLower(token) + "/" + strings.ToLower(holder) }

type call struct {
	ID     int               `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

// blockHash resolves an eth_call block argument: an EIP-1898 object with a
// block hash, a block number, or the latest tag. It returns the height too,
// -1 when the argument named a hash.
func (node *Node) blockHash(raw json.RawMessage) (string, int64) {
	var tag string
	if json.Unmarshal(raw, &tag) == nil {
		height := node.head
		if tag != "latest" {
			if _, err := fmt.Sscanf(tag, "0x%x", &height); err != nil {
				return "", -1
			}
		}
		return node.hashes[height], height
	}
	var block struct {
		BlockHash string `json:"blockHash"`
	}
	_ = json.Unmarshal(raw, &block) //nolint:errcheck // malformed calls read as empty
	return block.BlockHash, -1
}

// blockReply answers eth_getBlockByNumber with a header-only block.
func (node *Node) blockReply(raw json.RawMessage) map[string]any {
	hash, height := node.blockHash(raw)
	if hash == "" {
		return nil
	}
	return map[string]any{
		"number": fmt.Sprintf("0x%x", height), "hash": hash, "timestamp": "0x1",
		"parentHash": "0x" + strings.Repeat("0", 64), "gasUsed": "0x0", "gasLimit": "0x0", "transactions": []any{},
	}
}

func (node *Node) serve(writer http.ResponseWriter, request *http.Request) {
	var calls []call
	if err := json.NewDecoder(request.Body).Decode(&calls); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	replies := make([]map[string]any, len(calls))
	for index, current := range calls {
		reply := map[string]any{"jsonrpc": "2.0", "id": current.ID}
		if current.Method == "eth_getBlockByNumber" {
			reply["result"] = node.blockReply(current.Params[0])
			replies[index] = reply
			continue
		}
		var target struct{ To, Data string }
		_ = json.Unmarshal(current.Params[0], &target) //nolint:errcheck // malformed calls read as empty
		blockHash, _ := node.blockHash(current.Params[1])
		node.calls++
		holder := "0x" + target.Data[len(target.Data)-40:]
		switch {
		case node.missing[blockHash]:
			reply["error"] = map[string]any{"code": -32000, "message": "missing trie node"}
		case node.reverting[strings.ToLower(target.To)]:
			reply["error"] = map[string]any{"code": 3, "message": "execution reverted"}
		default:
			balance := node.balances[blockHash][key(target.To, holder)]
			if balance == nil {
				balance = new(big.Int)
			}
			reply["result"] = fmt.Sprintf("0x%064x", balance)
		}
		replies[index] = reply
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(replies) //nolint:errcheck // test server
}
