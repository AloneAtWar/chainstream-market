package chdb

import (
	"context"
	"testing"
)

func TestConnectValidatesOptions(t *testing.T) {
	ctx := context.Background()
	if _, err := Connect(ctx, Options{Database: "market"}); err == nil {
		t.Fatal("Connect() without address error = nil")
	}
	if _, err := Connect(ctx, Options{Addr: []string{"127.0.0.1:9000"}, Database: "market; DROP"}); err == nil {
		t.Fatal("Connect() with invalid database error = nil")
	}
}
