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
// calls at a missing block fail like a node without that state.
type Node struct {
	*httptest.Server
	mu        sync.Mutex
	balances  map[string]map[string]*big.Int
	reverting map[string]bool
	missing   map[string]bool
	calls     int
}

func NewNode() *Node {
	node := &Node{
		balances: make(map[string]map[string]*big.Int), reverting: make(map[string]bool), missing: make(map[string]bool),
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
	Params []json.RawMessage `json:"params"`
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
		var target struct{ To, Data string }
		var block struct {
			BlockHash string `json:"blockHash"`
		}
		_ = json.Unmarshal(current.Params[0], &target) //nolint:errcheck // malformed calls read as empty
		_ = json.Unmarshal(current.Params[1], &block)  //nolint:errcheck // malformed calls read as empty
		node.calls++
		reply := map[string]any{"jsonrpc": "2.0", "id": current.ID}
		holder := "0x" + target.Data[len(target.Data)-40:]
		switch {
		case node.missing[block.BlockHash]:
			reply["error"] = map[string]any{"code": -32000, "message": "missing trie node"}
		case node.reverting[strings.ToLower(target.To)]:
			reply["error"] = map[string]any{"code": 3, "message": "execution reverted"}
		default:
			balance := node.balances[block.BlockHash][key(target.To, holder)]
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
