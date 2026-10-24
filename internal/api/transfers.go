package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

// WithTransfers serves raw ERC-20 transfers from reader.
func (server *Server) WithTransfers(reader storage.TransferReader) *Server {
	server.transfers = reader
	return server
}

type transferResponse struct {
	BlockHeight      int64  `json:"block_height"`
	BlockTime        int64  `json:"block_time"`
	TransactionHash  string `json:"transaction_hash"`
	TransactionIndex int64  `json:"transaction_index"`
	LogIndex         uint32 `json:"log_index"`
	Token            string `json:"token"`
	From             string `json:"from"`
	To               string `json:"to"`
	AmountRaw        string `json:"amount_raw"`
}

// transferCursor continues a page: pass both values back as query
// parameters.
type transferCursor struct {
	BeforeHeight   int64  `json:"before_height"`
	BeforeLogIndex uint32 `json:"before_log_index"`
}

type transfersResponse struct {
	ChainID   uint64             `json:"chain_id"`
	Transfers []transferResponse `json:"transfers"`
	// Next is set when the page is full and more transfers may follow.
	Next *transferCursor `json:"next,omitempty"`
}

// transfersBy returns ERC-20 transfers newest first, by the path's token or
// holder, optionally narrowed by the other as a query parameter. Amounts are
// raw token units, and the native pseudo-address is not mapped, since only
// ERC-20 transfers are stored.
func (server *Server) transfersBy(pathName string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		query, err := transferQuery(request, pathName)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		transfers, err := server.transfers.Transfers(request.Context(), query)
		if err != nil {
			server.fail(writer, request, err)
			return
		}
		response := transfersResponse{ChainID: query.ChainID, Transfers: make([]transferResponse, len(transfers))}
		for index, transfer := range transfers {
			response.Transfers[index] = transferResponse{
				BlockHeight: transfer.BlockHeight, BlockTime: transfer.BlockTime.Unix(),
				TransactionHash: transfer.TransactionHash, TransactionIndex: transfer.TransactionIndex,
				LogIndex: transfer.LogIndex, Token: transfer.Token, From: transfer.From, To: transfer.To,
				AmountRaw: transfer.Amount.String(),
			}
		}
		if len(transfers) == query.Limit {
			last := transfers[len(transfers)-1]
			response.Next = &transferCursor{BeforeHeight: last.BlockHeight, BeforeLogIndex: last.LogIndex}
		}
		writeJSON(writer, http.StatusOK, response)
	}
}

func transferQuery(request *http.Request, pathName string) (storage.TransferQuery, error) {
	chain, err := chainID(request)
	if err != nil {
		return storage.TransferQuery{}, err
	}
	key := strings.ToLower(strings.TrimSpace(request.PathValue(pathName)))
	if key == "" {
		return storage.TransferQuery{}, badRequest(pathName + " is required")
	}
	count, err := limit(request)
	if err != nil {
		return storage.TransferQuery{}, err
	}
	query := storage.TransferQuery{ChainID: chain, Limit: count}
	values := request.URL.Query()
	if pathName == "token" {
		query.Token, query.Holder = key, strings.ToLower(strings.TrimSpace(values.Get("holder")))
	} else {
		query.Holder, query.Token = key, strings.ToLower(strings.TrimSpace(values.Get("token")))
	}
	if raw := values.Get("before_height"); raw != "" {
		height, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || height <= 0 {
			return storage.TransferQuery{}, badRequest("before_height must be a positive height")
		}
		query.BeforeHeight = height
	}
	if raw := values.Get("before_log_index"); raw != "" {
		index, parseErr := strconv.ParseUint(raw, 10, 32)
		if parseErr != nil || query.BeforeHeight == 0 {
			return storage.TransferQuery{}, badRequest("before_log_index must be a log index and needs before_height")
		}
		logIndex := uint32(index)
		query.BeforeLogIndex = &logIndex
	}
	return query, nil
}
