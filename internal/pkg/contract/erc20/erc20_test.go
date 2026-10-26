package erc20

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestBalanceOfSelector(t *testing.T) {
	data := NewToken().PackBalanceOf(common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	want := "70a08231000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := hex.EncodeToString(data); got != want {
		t.Fatalf("balanceOf calldata = %s, want %s", got, want)
	}
}
