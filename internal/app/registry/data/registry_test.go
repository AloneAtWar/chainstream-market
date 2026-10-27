package data

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/registry/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"gorm.io/gorm"
)

const (
	usdc = "0x1111111111111111111111111111111111111111"
	weth = "0x2222222222222222222222222222222222222222"
	pair = "0x3333333333333333333333333333333333333333"
)

func openDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	db, err := pgdb.Open(context.Background(), pgdb.Options{
		DSN: dsn, Schema: fmt.Sprintf("registry_test_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { pgdb.Close(db) })
	return db
}

type fixture struct {
	t        *testing.T
	db       *gorm.DB
	registry *biz.Registry
	offset   int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fixture := &fixture{t: t, db: openDB(t)}
	fixture.restart()
	return fixture
}

func (f *fixture) restart() {
	f.t.Helper()
	var err error
	f.registry, err = biz.New(biz.Options{ChainID: 1, Repo: NewRegistryRepo(f.db), HistoryDepth: 8})
	if err != nil {
		f.t.Fatalf("New() error = %v", err)
	}
	if _, _, err := f.registry.Restore(context.Background(), 1); err != nil {
		f.t.Fatalf("Restore() error = %v", err)
	}
}

func erc20(address, name, symbol string, decimals, sourceHeight int64) *streamv1.AssetFact {
	metadata := fmt.Sprintf(`{"Address":%q,"Name":{"Value":%q,"Known":true},"Symbol":{"Value":%q,"Known":true},`+
		`"Decimals":{"Value":%d,"Known":true},"Status":2}`, address, name, symbol, decimals)
	return &streamv1.AssetFact{
		Address: address, Standard: "erc20", Recognition: "recognized", MetadataComplete: true,
		Type: streamv1.AssetFactType_ASSET_FACT_TYPE_OBSERVED, SourceHeight: sourceHeight, MetadataJson: []byte(metadata),
	}
}

func created(fact *streamv1.AssetFact, height int64) *streamv1.AssetFact {
	fact.Type = streamv1.AssetFactType_ASSET_FACT_TYPE_CREATED
	fact.Deployment = &streamv1.DeploymentRef{BlockHeight: height, BlockTime: 1_700_000_000 + height, TransactionHash: "0xDEAD"}
	return fact
}

func poolFact(id string, sourceHeight int64, metadata string, tokens ...string) *streamv1.PoolFact {
	fact := &streamv1.PoolFact{
		PoolId: id, Protocol: "uniswap_v2", DexAddress: "0xFACTORY", TokenAddresses: tokens,
		SourceHeight: sourceHeight, SourceTime: 1_700_000_000 + sourceHeight,
	}
	if metadata != "" {
		fact.MetadataJson = []byte(metadata)
	}
	return fact
}

func (f *fixture) apply(height int64, assets []*streamv1.AssetFact, pools ...*streamv1.PoolFact) {
	f.t.Helper()
	block := &streamv1.BlockEvent{
		ChainId: 1, BlockHeight: height, BlockHash: fmt.Sprintf("0x%d", height),
		Assets: assets, Pools: pools, FinalizedHeight: height - 20,
	}
	if err := f.registry.Apply(context.Background(), block); err != nil {
		f.t.Fatalf("Apply(%d) error = %v", height, err)
	}
	f.commit(height, block.GetBlockHash())
}

func (f *fixture) revert(rollbackTo int64) error {
	f.t.Helper()
	if err := f.registry.Revert(context.Background(), rollbackTo); err != nil {
		return err
	}
	f.commit(rollbackTo, "")
	return nil
}

func (f *fixture) commit(height int64, hash string) {
	f.t.Helper()
	checkpoint := chainsync.Checkpoint{ChainID: 1, Offset: f.offset, Height: height, Hash: hash}
	if err := f.registry.Commit(context.Background(), checkpoint); err != nil {
		f.t.Fatalf("Commit() error = %v", err)
	}
	f.offset++
}

func (f *fixture) token(address string) (model.Token, bool) {
	f.t.Helper()
	var row pgdb.TokenRow
	err := f.db.Select(pgdb.TokenColumns).Where("chain_id = 1 AND address = ?", address).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Token{}, false
	}
	if err != nil {
		f.t.Fatalf("read token error = %v", err)
	}
	token, err := row.Token()
	if err != nil {
		f.t.Fatalf("Token() error = %v", err)
	}
	return token, true
}

func (f *fixture) symbol(address string) string {
	f.t.Helper()
	token, ok := f.token(address)
	if !ok {
		return "<none>"
	}
	return *token.Symbol
}

func (f *fixture) pool(id string) (model.Pool, bool) {
	f.t.Helper()
	var row pgdb.PoolRow
	err := f.db.Select(pgdb.PoolColumns).Where("chain_id = 1 AND pool_id = ?", id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Pool{}, false
	}
	if err != nil {
		f.t.Fatalf("read pool error = %v", err)
	}
	return row.Pool(), true
}

func (f *fixture) tokenPools(token string) []string {
	f.t.Helper()
	var ids []string
	err := f.db.Model(&pgdb.PoolRow{}).Where("chain_id = 1 AND tokens @> ?::text[]", pgdb.Array([]string{token})).
		Order("source_height DESC, pool_id").Limit(10).Pluck("pool_id", &ids).Error
	if err != nil {
		f.t.Fatalf("read token pools error = %v", err)
	}
	return ids
}

func TestRegistryStoresFacts(t *testing.T) {
	f := newFixture(t)
	f.apply(1, []*streamv1.AssetFact{
		created(erc20("0x1111111111111111111111111111111111111111", "USD Coin", "USDC", 6, 1), 1),
		{
			Address: "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE", Standard: "native", Recognition: "recognized",
			MetadataComplete: true, MetadataJson: []byte(`{"Address":"0xeeee","Name":"Ether","Symbol":"ETH","Decimals":18,"Status":2}`),
		},
	}, poolFact("0x3333333333333333333333333333333333333333", 1, `{"fee":3000}`, usdc, weth))

	token, ok := f.token(usdc)
	if !ok || *token.Name != "USD Coin" || *token.Symbol != "USDC" || *token.Decimals != 6 || !token.Created ||
		token.Deployment == nil || token.Deployment.TransactionHash != "0xdead" || token.FirstHeight != 1 ||
		token.Standard != "erc20" || !token.MetadataComplete || len(token.Metadata) == 0 {
		t.Fatalf("token = %+v", token)
	}
	native, ok := f.token("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if !ok || *native.Symbol != "ETH" || *native.Decimals != 18 || native.Created || native.SourceHeight != 1 {
		t.Fatalf("native = %+v", native)
	}
	pool, ok := f.pool(pair)
	if !ok || pool.Protocol != "uniswap_v2" || pool.DexAddress != "0xfactory" || !slices.Equal(pool.Tokens, []string{usdc, weth}) ||
		string(pool.Metadata) != `{"fee": 3000}` || pool.SourceTime != 1_700_000_001 {
		t.Fatalf("pool = %+v", pool)
	}
	if got := f.tokenPools(weth); !slices.Equal(got, []string{pair}) {
		t.Fatalf("TokenPools(weth) = %v", got)
	}
	if got := f.tokenPools(pair); len(got) != 0 {
		t.Fatalf("TokenPools(pair) = %v", got)
	}
}

func TestRegistryNewerMetadataWins(t *testing.T) {
	f := newFixture(t)
	f.apply(1, []*streamv1.AssetFact{created(erc20(usdc, "USD Coin", "USDC", 6, 1), 1)})
	f.apply(2, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "USDC2", 6, 2)})
	// A late observation read at an older height does not replace newer
	// metadata.
	f.apply(3, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "OLD", 6, 1)})
	token, _ := f.token(usdc)
	if *token.Symbol != "USDC2" || token.UpdatedHeight != 2 || token.FirstHeight != 1 || !token.Created || token.Deployment == nil {
		t.Fatalf("token = %+v", token)
	}
	// Within one block the fact read at the higher height wins, and a
	// deployment from either is kept.
	f.apply(4, []*streamv1.AssetFact{erc20(weth, "Wrapped", "W4", 18, 4), created(erc20(weth, "Wrapped", "W3", 18, 3), 3)})
	if got, _ := f.token(weth); *got.Symbol != "W4" || !got.Created || got.Deployment.BlockHeight != 3 {
		t.Fatalf("weth = %+v", got)
	}

	f.apply(5, nil, poolFact(pair, 5, `{"fee":3000}`, usdc, weth))
	f.apply(6, nil, poolFact(pair, 4, "", usdc))
	f.apply(7, nil, poolFact(pair, 7, "", weth, usdc))
	pool, _ := f.pool(pair)
	if !slices.Equal(pool.Tokens, []string{weth, usdc}) || pool.SourceHeight != 7 || string(pool.Metadata) != `{"fee": 3000}` ||
		pool.FirstHeight != 5 || pool.UpdatedHeight != 7 {
		t.Fatalf("pool = %+v", pool)
	}
}

// notRecognized is the fact chainstream-evm publishes for a standard whose
// probes the address did not answer.
func notRecognized(address, standard string, sourceHeight int64) *streamv1.AssetFact {
	return &streamv1.AssetFact{
		Address: address, Standard: standard, Recognition: "not_recognized",
		Type: streamv1.AssetFactType_ASSET_FACT_TYPE_OBSERVED, SourceHeight: sourceHeight,
		MetadataJson: []byte(fmt.Sprintf(`{"Address":%q,"Status":1}`, address)),
	}
}

func TestRegistryKeepsTheRecognizedStandard(t *testing.T) {
	f := newFixture(t)
	// chainstream-evm reports WETH once per probed standard in one block.
	f.apply(1, []*streamv1.AssetFact{
		erc20(weth, "Wrapped Ether", "WETH", 18, 1), notRecognized(weth, "erc721", 1), notRecognized(weth, "erc1155", 1),
	})
	if got, _ := f.token(weth); got.Standard != "erc20" || got.Recognition != "recognized" || *got.Symbol != "WETH" {
		t.Fatalf("weth = %+v", got)
	}
	// A later block's facts for other standards do not replace it either.
	f.apply(2, []*streamv1.AssetFact{notRecognized(weth, "erc1155", 2)})
	if got, _ := f.token(weth); got.Standard != "erc20" || *got.Symbol != "WETH" || got.UpdatedHeight != 1 {
		t.Fatalf("weth after erc1155 = %+v", got)
	}
	// The recognized standard itself can still change the row, and an
	// address nothing recognized takes the latest fact.
	f.apply(3, []*streamv1.AssetFact{erc20(weth, "Wrapped Ether", "WETH9", 18, 3)})
	f.apply(4, []*streamv1.AssetFact{notRecognized(usdc, "erc721", 4), notRecognized(usdc, "erc20", 4)})
	if got, _ := f.token(weth); *got.Symbol != "WETH9" {
		t.Fatalf("weth after update = %+v", got)
	}
	if got, ok := f.token(usdc); !ok || got.Standard != "erc20" || got.Recognition != "not_recognized" {
		t.Fatalf("usdc = %+v", got)
	}
	f.apply(5, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "USDC", 6, 5)})
	if got, _ := f.token(usdc); got.Recognition != "recognized" || *got.Symbol != "USDC" {
		t.Fatalf("usdc after recognition = %+v", got)
	}
}

func TestRegistryRevertsAndResumes(t *testing.T) {
	f := newFixture(t)
	f.apply(1, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "A", 6, 1)})
	f.apply(2, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "B", 6, 2)})
	f.apply(3, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "C", 6, 3), erc20(weth, "Wrapped", "WETH", 18, 3)},
		poolFact(pair, 3, "", usdc, weth))
	f.apply(4, []*streamv1.AssetFact{created(erc20(usdc, "USD Coin", "D", 6, 4), 4)})

	if err := f.revert(2); err != nil {
		t.Fatalf("revert(2) error = %v", err)
	}
	token, _ := f.token(usdc)
	if *token.Symbol != "B" || token.UpdatedHeight != 2 || token.Created || token.Deployment != nil {
		t.Fatalf("token after revert = %+v", token)
	}
	if _, ok := f.token(weth); ok {
		t.Fatal("weth survived the revert of the block that created it")
	}
	if _, ok := f.pool(pair); ok {
		t.Fatal("pool survived the revert of the block that created it")
	}

	// The replacement branch applies on top, and a restart resumes from
	// the checkpoint with its undo history.
	f.apply(3, []*streamv1.AssetFact{erc20(usdc, "USD Coin", "C2", 6, 3)})
	f.restart()
	if err := f.revert(2); err != nil {
		t.Fatalf("revert(2) after restart error = %v", err)
	}
	if got := f.symbol(usdc); got != "B" {
		t.Fatalf("symbol after second revert = %s", got)
	}
	if err := f.revert(0); err != nil {
		t.Fatalf("revert(0) error = %v", err)
	}
	if got := f.symbol(usdc); got != "<none>" {
		t.Fatalf("symbol after full revert = %s", got)
	}

	// Undo history older than history_depth is pruned, and a reorg below
	// it is refused.
	for height := int64(1); height <= 20; height++ {
		f.apply(height, []*streamv1.AssetFact{erc20(usdc, "USD Coin", fmt.Sprint("S", height), 6, height)})
	}
	f.restart()
	if err := f.revert(11); !errors.Is(err, biz.ErrReorgTooDeep) {
		t.Fatalf("revert(11) error = %v, want ErrReorgTooDeep", err)
	}
	if err := f.revert(12); err != nil {
		t.Fatalf("revert(12) error = %v", err)
	}
	if got := f.symbol(usdc); got != "S12" {
		t.Fatalf("symbol after revert(12) = %s", got)
	}
}

func TestRegistryCleansMetadata(t *testing.T) {
	f := newFixture(t)
	fact := erc20(usdc, "Evil\\u0000Coin", "EV\\u0000L", 6, 1)
	fact.MetadataJson = []byte(`{"Name":{"Value":"Evil\u0000Coin","Known":true},"Symbol":{"Value":"EV\u0000L","Known":true},"Decimals":{"Value":0,"Known":false}}`)
	broken := erc20(weth, "x", "x", 18, 1)
	broken.MetadataJson = []byte(`{not json`)
	f.apply(1, []*streamv1.AssetFact{fact, broken}, poolFact(pair, 1, "null", usdc))

	token, _ := f.token(usdc)
	if *token.Name != "EvilCoin" || *token.Symbol != "EVL" || token.Decimals != nil {
		t.Fatalf("token = %+v", token)
	}
	if got, ok := f.token(weth); !ok || got.Metadata != nil || got.Symbol != nil {
		t.Fatalf("broken = %+v, %v", got, ok)
	}
	if pool, _ := f.pool(pair); pool.Metadata != nil {
		t.Fatalf("pool metadata = %s", pool.Metadata)
	}
}
