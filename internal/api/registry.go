package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// WithRegistry serves token and pool metadata from reader, and converts
// holder balances with each token's decimals.
func (server *Server) WithRegistry(reader storage.RegistryReader) *Server {
	server.registry = reader
	return server
}

type tokenResponse struct {
	ChainID          uint64            `json:"chain_id"`
	Address          string            `json:"address"`
	Standard         string            `json:"standard"`
	Recognition      string            `json:"recognition"`
	MetadataComplete bool              `json:"metadata_complete"`
	Name             *string           `json:"name"`
	Symbol           *string           `json:"symbol"`
	Decimals         *uint8            `json:"decimals"`
	Created          bool              `json:"created"`
	Deployment       *model.Deployment `json:"deployment,omitempty"`
	SourceHeight     int64             `json:"source_height"`
	FirstHeight      int64             `json:"first_height"`
	UpdatedHeight    int64             `json:"updated_height"`
	Metadata         json.RawMessage   `json:"metadata,omitempty"`
}

type poolResponse struct {
	ChainID       uint64          `json:"chain_id"`
	PoolID        string          `json:"pool_id"`
	Protocol      string          `json:"protocol"`
	DexAddress    string          `json:"dex_address"`
	Tokens        []string        `json:"tokens"`
	Created       bool            `json:"created"`
	SourceHeight  int64           `json:"source_height"`
	SourceTime    int64           `json:"source_time"`
	FirstHeight   int64           `json:"first_height"`
	UpdatedHeight int64           `json:"updated_height"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

func newPoolResponse(chain uint64, pool model.Pool) poolResponse {
	return poolResponse{
		ChainID: chain, PoolID: pool.PoolID, Protocol: pool.Protocol, DexAddress: pool.DexAddress,
		Tokens: pool.Tokens, Created: pool.Created, SourceHeight: pool.SourceHeight, SourceTime: pool.SourceTime,
		FirstHeight: pool.FirstHeight, UpdatedHeight: pool.UpdatedHeight, Metadata: pool.Metadata,
	}
}

// pathAddress lowercases a path address without mapping aliases, since the
// registry keeps the native pseudo-address as its own asset.
func pathAddress(request *http.Request, name string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(request.PathValue(name)))
	if value == "" {
		return "", badRequest(name + " is required")
	}
	return value, nil
}

func (server *Server) token(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	address, err := pathAddress(request, "token")
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	token, ok, err := server.registry.Token(request.Context(), chain, address)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "unknown token"})
		return
	}
	writeJSON(writer, http.StatusOK, tokenResponse{
		ChainID: chain, Address: token.Address, Standard: token.Standard, Recognition: token.Recognition,
		MetadataComplete: token.MetadataComplete, Name: token.Name, Symbol: token.Symbol, Decimals: token.Decimals,
		Created: token.Created, Deployment: token.Deployment, SourceHeight: token.SourceHeight,
		FirstHeight: token.FirstHeight, UpdatedHeight: token.UpdatedHeight, Metadata: token.Metadata,
	})
}

func (server *Server) tokenPools(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	address, err := pathAddress(request, "token")
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	pools, err := server.registry.TokenPools(request.Context(), chain, address, count)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	body := make([]poolResponse, len(pools))
	for index, pool := range pools {
		body[index] = newPoolResponse(chain, pool)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"pools": body})
}

func (server *Server) pool(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	poolID, err := pathAddress(request, "pool")
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	pool, ok, err := server.registry.Pool(request.Context(), chain, poolID)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "unknown pool"})
		return
	}
	writeJSON(writer, http.StatusOK, newPoolResponse(chain, pool))
}
