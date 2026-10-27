package service

import (
	"net/http"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/asset"
)

// nativeDecimals is the decimals of ETH and BNB.
const nativeDecimals = 18

// WithHolders serves holder queries from reader.
func (server *Server) WithHolders(reader HolderReader) *Server {
	server.holders = reader
	return server
}

type holderResponse struct {
	Holder     string `json:"holder"`
	BalanceRaw string `json:"balance_raw"`
	// Balance is BalanceRaw in whole tokens, present when decimals are known.
	Balance string `json:"balance,omitempty"`
}

type holdersResponse struct {
	ChainID     uint64           `json:"chain_id"`
	Token       string           `json:"token"`
	Decimals    *uint8           `json:"decimals,omitempty"`
	HolderCount int64            `json:"holder_count"`
	Holders     []holderResponse `json:"holders"`
}

// tokenHolders returns a token's holder count and largest holders. Balances
// are raw token units, and also whole tokens when the registry knows the
// token's decimals. The native pseudo-address is the native coin itself, not
// its wrapped token, and has 18 decimals on every supported chain.
func (server *Server) tokenHolders(writer http.ResponseWriter, request *http.Request) {
	chain, err := chainID(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	token := strings.ToLower(strings.TrimSpace(request.PathValue("token")))
	if token == "" {
		server.fail(writer, request, badRequest("token is required"))
		return
	}
	count, err := limit(request)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	total, err := server.holders.HolderCount(request.Context(), chain, token)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	top, err := server.holders.TopHolders(request.Context(), chain, token, count)
	if err != nil {
		server.fail(writer, request, err)
		return
	}
	response := holdersResponse{ChainID: chain, Token: token, HolderCount: total, Holders: make([]holderResponse, len(top))}
	if token == asset.NativeAddress {
		decimals := uint8(nativeDecimals)
		response.Decimals = &decimals
	} else if server.registry != nil {
		metadata, ok, registryErr := server.registry.Token(request.Context(), chain, token)
		if registryErr != nil {
			server.fail(writer, request, registryErr)
			return
		}
		if ok {
			response.Decimals = metadata.Decimals
		}
	}
	for index, holding := range top {
		response.Holders[index] = holderResponse{Holder: holding.Holder, BalanceRaw: holding.Balance.String()}
		if response.Decimals != nil {
			response.Holders[index].Balance = decimal.NewFromBigInt(holding.Balance, -int32(*response.Decimals)).String()
		}
	}
	writeJSON(writer, http.StatusOK, response)
}
