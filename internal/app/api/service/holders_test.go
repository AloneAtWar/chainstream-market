package service

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
)

type fakeHolders struct {
	token string
	limit int
	err   error
}

func (fake *fakeHolders) TopHolders(_ context.Context, _ uint64, token string, limit int) ([]model.Holding, error) {
	fake.token, fake.limit = token, limit
	return []model.Holding{{HolderKey: model.HolderKey{Token: token, Holder: "0xaa"}, Balance: big.NewInt(1200)}}, fake.err
}

func (fake *fakeHolders) HolderCount(context.Context, uint64, string) (int64, error) {
	return 3, fake.err
}

func serveHolders(t *testing.T, holders HolderReader, path string) int {
	t.Helper()
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil)
	if holders != nil {
		server.WithHolders(holders)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return recorder.Code
}

func TestTokenHolders(t *testing.T) {
	holders := &fakeHolders{}
	server := New(&fakeReader{}, []time.Duration{time.Minute}, nil).WithHolders(holders)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/v1/chains/1/tokens/0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE/holders?limit=5", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	// The native coin always has 18 decimals.
	want := `{"chain_id":1,"token":"0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","decimals":18,"holder_count":3,` +
		`"holders":[{"holder":"0xaa","balance_raw":"1200","balance":"0.0000000000000012"}]}` + "\n"
	if recorder.Body.String() != want {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	// The native pseudo-address is not mapped to the wrapped token.
	if holders.token != "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" || holders.limit != 5 {
		t.Fatalf("query = %s %d", holders.token, holders.limit)
	}
}

func TestTokenHoldersErrors(t *testing.T) {
	if code := serveHolders(t, nil, "/v1/chains/1/tokens/0xaa/holders"); code != http.StatusNotFound {
		t.Fatalf("without a holder store status = %d", code)
	}
	if code := serveHolders(t, &fakeHolders{}, "/v1/chains/1/tokens/0xaa/holders?limit=0"); code != http.StatusBadRequest {
		t.Fatalf("bad limit status = %d", code)
	}
	if code := serveHolders(t, &fakeHolders{err: errors.New("down")}, "/v1/chains/1/tokens/0xaa/holders"); code != http.StatusInternalServerError {
		t.Fatalf("store error status = %d", code)
	}
}
