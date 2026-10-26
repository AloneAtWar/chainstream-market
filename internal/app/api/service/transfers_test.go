package service

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type fakeTransfers struct {
	query model.TransferQuery
	err   error
}

func (fake *fakeTransfers) Transfers(_ context.Context, query model.TransferQuery) ([]model.Transfer, error) {
	fake.query = query
	transfer := model.Transfer{BlockHeight: 101, BlockTime: time.Unix(1_700_000_000, 0), RawTransfer: model.RawTransfer{
		TransactionHash: "0xtx", TransactionIndex: 4, LogIndex: 7, Token: "0xt", From: "0xa", To: "0xb", Amount: big.NewInt(5),
	}}
	return []model.Transfer{transfer}, fake.err
}

func serveTransfers(t *testing.T, transfers TransferReader, path string) *httptest.ResponseRecorder {
	t.Helper()
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil)
	if transfers != nil {
		server.WithTransfers(transfers)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return recorder
}

func TestTokenTransfers(t *testing.T) {
	transfers := &fakeTransfers{}
	recorder := serveTransfers(t, transfers, "/v1/chains/1/tokens/0xT/transfers?limit=1&before_height=200&before_log_index=3&holder=0xA")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	want := `{"chain_id":1,"transfers":[{"block_height":101,"block_time":1700000000,"transaction_hash":"0xtx",` +
		`"transaction_index":4,"log_index":7,"token":"0xt","from":"0xa","to":"0xb","amount_raw":"5"}],` +
		`"next":{"before_height":101,"before_log_index":7}}` + "\n"
	if recorder.Body.String() != want {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	query := transfers.query
	if query.Token != "0xt" || query.Holder != "0xa" || query.BeforeHeight != 200 || *query.BeforeLogIndex != 3 || query.Limit != 1 {
		t.Fatalf("query = %+v", query)
	}
}

func TestHolderTransfers(t *testing.T) {
	transfers := &fakeTransfers{}
	recorder := serveTransfers(t, transfers, "/v1/chains/56/holders/0xA/transfers?token=0xT")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	// A partial page has no cursor.
	if body := recorder.Body.String(); strings.Contains(body, `"next"`) {
		t.Fatalf("body = %s", body)
	}
	query := transfers.query
	if query.ChainID != 56 || query.Holder != "0xa" || query.Token != "0xt" || query.BeforeHeight != 0 || query.BeforeLogIndex != nil {
		t.Fatalf("query = %+v", query)
	}
}

func TestTransfersErrors(t *testing.T) {
	tests := map[string]struct {
		reader TransferReader
		path   string
		want   int
	}{
		"no store":      {nil, "/v1/chains/1/tokens/0xt/transfers", http.StatusNotFound},
		"bad chain":     {&fakeTransfers{}, "/v1/chains/x/tokens/0xt/transfers", http.StatusBadRequest},
		"bad height":    {&fakeTransfers{}, "/v1/chains/1/tokens/0xt/transfers?before_height=0", http.StatusBadRequest},
		"log no height": {&fakeTransfers{}, "/v1/chains/1/holders/0xa/transfers?before_log_index=1", http.StatusBadRequest},
		"store error":   {&fakeTransfers{err: errors.New("down")}, "/v1/chains/1/holders/0xa/transfers", http.StatusInternalServerError},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if code := serveTransfers(t, test.reader, test.path).Code; code != test.want {
				t.Fatalf("status = %d, want %d", code, test.want)
			}
		})
	}
}
