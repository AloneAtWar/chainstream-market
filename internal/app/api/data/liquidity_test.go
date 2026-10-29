package data

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
)

func TestLiquidityReaderReadsEngineTickTables(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	db, err := pgdb.Open(ctx, pgdb.Options{DSN: dsn, Schema: fmt.Sprintf("api_liquidity_test_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatalf("pgdb.Open() error = %v", err)
	}
	t.Cleanup(func() { pgdb.Close(db) })
	rows := [][3]string{
		{model.StateKindCLAMMPools, "0xpool", `{"protocol":"uniswap_v4","fee":8388608,"snapshot_height":9,"sqrt_price_x96":"79228162514264337593543950336","swap_delta_hooks":true}`},
		{model.StateKindTicks, model.TickStateKey("0xpool", -60), `"5000"`},
		{model.StateKindTicks, model.TickStateKey("0xpool", 60), `"-5000"`},
		{model.StateKindTicks, model.TickStateKey("0xpool2", -10), `"7"`},
		{model.StateKindCLAMMPools, "0xpool2", `{"protocol":"uniswap_v3"}`},
	}
	for _, row := range rows {
		if err = db.Exec(`INSERT INTO engine_state (chain_id, kind, key, value) VALUES (1, ?, ?, ?::jsonb)`, row[0], row[1], row[2]).Error; err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	reader := NewLiquidityReader(&Data{state: db})
	table, ok, err := reader.PoolTickTable(ctx, 1, "0xpool")
	if err != nil || !ok {
		t.Fatalf("PoolTickTable() = %t, %v", ok, err)
	}
	if table.Pool.Protocol != "uniswap_v4" || table.Pool.Fee != model.DynamicFeeFlag || !table.Pool.SwapDeltaHooks ||
		len(table.Ticks) != 2 || table.Ticks[-60].String() != "5000" || table.Ticks[60].String() != "-5000" {
		t.Fatalf("table = %+v", table)
	}
	if _, ok, err := reader.PoolTickTable(ctx, 1, "0xmissing"); err != nil || ok {
		t.Fatalf("missing pool = %t, %v", ok, err)
	}
	if _, ok, err := reader.PoolTickTable(ctx, 2, "0xpool"); err != nil || ok {
		t.Fatalf("other chain = %t, %v", ok, err)
	}
}
