package data

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/app/wallet/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/live"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/durationpb"
)

const liveTopic = "chainstream-eth-live"

var (
	alice  = "0x" + strings.Repeat("a", 40)
	bob    = "0x" + strings.Repeat("b", 40)
	tokenX = "0x" + strings.Repeat("1", 40)
	tokenY = "0x" + strings.Repeat("2", 40)
	weth   = "0x" + strings.Repeat("e", 40)
)

// openData 连接 POSTGRES_DSN 的一个新 schema 和 CLICKHOUSE_ADDR 的一个新库，两个都设置了
// 才运行。
func openData(t *testing.T) *Data {
	t.Helper()
	dsn, addr := os.Getenv("POSTGRES_DSN"), os.Getenv("CLICKHOUSE_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("POSTGRES_DSN and CLICKHOUSE_ADDR are needed")
	}
	ctx := context.Background()
	name := fmt.Sprintf("wallet_test_%d", time.Now().UnixNano())
	state, err := pgdb.Open(ctx, pgdb.Options{DSN: dsn, Schema: name})
	if err != nil {
		t.Fatalf("pgdb.Open() error = %v", err)
	}
	options := chdb.Options{
		Addr: strings.Split(addr, ","), Database: name,
		Username: os.Getenv("CLICKHOUSE_USER"), Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	market, err := chdb.Connect(ctx, options)
	if err != nil {
		t.Fatalf("chdb.Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := state.Exec("DROP SCHEMA IF EXISTS " + name + " CASCADE").Error; err != nil {
			t.Errorf("drop schema: %v", err)
		}
		pgdb.Close(state)
		if err := market.Exec("DROP DATABASE IF EXISTS " + name).Error; err != nil {
			t.Errorf("drop database: %v", err)
		}
		if err := chdb.Close(market); err != nil {
			t.Errorf("chdb.Close() error = %v", err)
		}
	})
	for _, schema := range [][]string{chdb.MarketSchema, chdb.WalletSchema} {
		if err := chdb.Migrate(ctx, market, schema); err != nil {
			t.Fatalf("chdb.Migrate() error = %v", err)
		}
	}
	if err := chdb.SetWalletTradesTTL(ctx, market, 120); err != nil {
		t.Fatal(err)
	}
	return &Data{market: market, state: state}
}

// engineTrade 是 market-engine 写进 ClickHouse trades 表的一笔成交。
func engineTrade(height int64, at time.Time, trader, token, side, amount, value string) chdb.TradeRow {
	return chdb.TradeRow{
		ChainID: 1, BlockHeight: height, BlockTime: at, TransactionHash: fmt.Sprintf("0x%064d", height),
		Protocol: "uniswap_v2", PoolID: "0xpool", Trader: trader, Side: side, BaseToken: token, QuoteToken: weth,
		BaseAmount: decimal.RequireFromString(amount), QuoteAmount: decimal.RequireFromString("1"),
		PriceQuote: decimal.RequireFromString("1"), PriceUSD: decimal.RequireFromString("1"),
		ValueUSD: decimal.RequireFromString(value), RevisionHeight: height,
	}
}

func liveTradeOf(row chdb.TradeRow) live.Trade {
	return live.Trade{
		BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.Unix(), RevisionHeight: row.RevisionHeight,
		TransactionHash: row.TransactionHash, Protocol: row.Protocol, PoolID: row.PoolID, Trader: row.Trader,
		Side: row.Side, BaseToken: row.BaseToken, QuoteToken: row.QuoteToken, BaseAmount: row.BaseAmount.String(),
		QuoteAmount: row.QuoteAmount.String(), PriceUSD: row.PriceUSD.String(), ValueUSD: row.ValueUSD.String(),
	}
}

type smokeChain struct {
	t        *testing.T
	data     *Data
	cluster  *kfake.Cluster
	producer *kgo.Client
	base     time.Time
}

func (chain *smokeChain) at(height int64) time.Time {
	return chain.base.Add(time.Duration(height) * 12 * time.Second)
}

// engine 把区块和成交写进 ClickHouse，就像 market-engine 一样。
func (chain *smokeChain) engine(height int64, trades ...chdb.TradeRow) {
	chain.t.Helper()
	ctx := context.Background()
	if len(trades) > 0 {
		if err := chdb.Insert(ctx, chain.data.market, &trades); err != nil {
			chain.t.Fatal(err)
		}
	}
	blocks := []chdb.BlockRow{{ChainID: 1, BlockHeight: height, BlockHash: fmt.Sprint(height), BlockTime: chain.at(height)}}
	if err := chdb.Insert(ctx, chain.data.market, &blocks); err != nil {
		chain.t.Fatal(err)
	}
}

// publish 把一条记录写进 live topic，返回它的 offset。
func (chain *smokeChain) publish(update live.Update) int64 {
	chain.t.Helper()
	update.ChainID = 1
	payload, err := json.Marshal(update)
	if err != nil {
		chain.t.Fatal(err)
	}
	record := &kgo.Record{Topic: liveTopic, Partition: 0, Value: payload}
	produced, err := chain.producer.ProduceSync(context.Background(), record).First()
	if err != nil {
		chain.t.Fatal(err)
	}
	return produced.Offset
}

func (chain *smokeChain) block(height int64, trades ...chdb.TradeRow) live.Update {
	update := live.Update{Type: live.UpdateBlock, Height: height, Parts: 1, BlockTime: chain.at(height).Unix(), FinalizedHeight: height - 64}
	for _, trade := range trades {
		update.Trades = append(update.Trades, liveTradeOf(trade))
	}
	return update
}

// follow 运行一次 Follower，直到检查点满足 until，然后停下；等待期间反复调用 whileWaiting。
func (chain *smokeChain) follow(until func(biz.Checkpoint) bool, whileWaiting func()) {
	chain.t.Helper()
	openers := NewOpeners(&config.Kafka{Brokers: chain.cluster.ListenAddrs(), FetchMaxBytes: 1 << 20,
		FetchMaxWait: durationpb.New(50 * time.Millisecond)}, []*config.Chain{{Name: "eth", ChainId: 1, LiveTopic: liveTopic}})
	repo := NewWalletRepo(chain.data)
	follower, err := biz.New(biz.Options{
		ChainID: 1, Chain: "eth", Open: openers[1], Repo: repo, Trades: NewTradeSource(chain.data),
		HistoryDepth: 16, History: 90 * 24 * time.Hour, FinalAge: time.Hour, BackfillHeights: 2, CloseRatio: 0.001,
		Now: func() time.Time { return chain.at(1000) },
	})
	if err != nil {
		chain.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- follower.Run(ctx) }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		checkpoint, ok, err := repo.LoadWalletCheckpoint(context.Background(), 1)
		if err != nil {
			chain.t.Fatal(err)
		}
		if ok && until(checkpoint) {
			break
		}
		if whileWaiting != nil {
			whileWaiting()
		}
		select {
		case runErr := <-done:
			cancel()
			chain.t.Fatalf("Run() stopped early: %v", runErr)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			chain.t.Fatalf("ledger did not catch up, checkpoint %+v", checkpoint)
		}
	}
	cancel()
	if err := <-done; err != nil {
		chain.t.Fatalf("Run() error = %v", err)
	}
}

func position(t *testing.T, data *Data, wallet, token string) (model.Position, bool) {
	t.Helper()
	positions, err := NewWalletRepo(data).Positions(context.Background(), 1, []model.WalletKey{{Wallet: wallet, Token: token}})
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) == 0 {
		return model.Position{}, false
	}
	return positions[0], true
}

func walletTrades(t *testing.T, data *Data) []chdb.WalletTradeRow {
	t.Helper()
	var rows []chdb.WalletTradeRow
	if err := data.market.Order("block_height, wallet, token").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestSmokeKafkaLedgerAndStats 在 kfake 的 live topic、真实的 PostgreSQL 和 ClickHouse 上跑
// market-wallet：首次启动回放历史、补 live topic 跳过的区块、回滚、重启续读，最后汇总
// 统计并打标签。
func TestSmokeKafkaLedgerAndStats(t *testing.T) {
	data := openData(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, liveTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	chain := &smokeChain{t: t, data: data, cluster: cluster, producer: producer, base: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}

	// 历史：market-engine 已经处理过 1-9，live topic 上没有（或已经读不到）。
	chain.engine(1, engineTrade(1, chain.at(1), alice, tokenX, "buy", "100", "100"))
	chain.engine(2)
	chain.engine(3, engineTrade(3, chain.at(3), alice, tokenX, "buy", "100", "300"), engineTrade(3, chain.at(3), bob, tokenY, "buy", "10", "10"))
	for height := int64(4); height <= 9; height++ {
		chain.engine(height)
	}
	// 首次启动从 live topic 末尾读：一直发区块 10，直到账本从它开始。
	sell10 := engineTrade(10, chain.at(10), alice, tokenX, "sell", "50", "150")
	chain.engine(10, sell10)
	chain.follow(func(checkpoint biz.Checkpoint) bool { return checkpoint.Height >= 10 }, func() { _ = chain.publish(chain.block(10, sell10)) })

	alicePosition, _ := position(t, data, alice, tokenX)
	if !alicePosition.Quantity.Equal(decimal.RequireFromString("150")) || !alicePosition.RealizedUSD.Equal(decimal.RequireFromString("50")) {
		t.Fatalf("after bootstrap: %+v", alicePosition)
	}

	// 11 和 12 被 live topic 跳过；13 到达时从 ClickHouse 补，之后 14 被回滚、换成新的 14。
	chain.engine(11, engineTrade(11, chain.at(11), bob, tokenY, "sell", "10", "30"))
	chain.engine(12)
	sell13 := engineTrade(13, chain.at(13), alice, tokenX, "sell", "100", "100")
	chain.engine(13, sell13)
	chain.publish(chain.block(13, sell13))
	chain.publish(chain.block(14, engineTrade(14, chain.at(14), alice, tokenX, "sell", "50", "500")))
	chain.publish(live.Update{Type: live.UpdateRevert, Height: 13})
	buy14 := engineTrade(14, chain.at(14), alice, tokenY, "buy", "5", "5")
	last := chain.publish(chain.block(14, buy14))
	chain.follow(func(checkpoint biz.Checkpoint) bool { return checkpoint.Offset >= last }, nil)

	// 重启：从检查点的下一条续读。
	sell15 := engineTrade(15, chain.at(15), alice, tokenY, "sell", "5", "50")
	last = chain.publish(chain.block(15, sell15))
	chain.follow(func(checkpoint biz.Checkpoint) bool { return checkpoint.Offset >= last }, nil)

	alicePosition, _ = position(t, data, alice, tokenX)
	// 均价 2：卖 50 收 150（+50），卖 100 收 100（-100），剩 50；被回滚的 14 不算。
	if !alicePosition.Quantity.Equal(decimal.RequireFromString("50")) || !alicePosition.RealizedUSD.Equal(decimal.RequireFromString("-50")) ||
		alicePosition.Wins != 1 || alicePosition.Losses != 1 {
		t.Fatalf("alice x: %+v", alicePosition)
	}
	bobPosition, _ := position(t, data, bob, tokenY)
	if !bobPosition.Quantity.IsZero() || !bobPosition.RealizedUSD.Equal(decimal.RequireFromString("20")) {
		t.Fatalf("bob y: %+v", bobPosition)
	}
	var heights []string
	for _, row := range walletTrades(t, data) {
		heights = append(heights, fmt.Sprintf("%d:%s", row.BlockHeight, row.Event))
	}
	want := []string{"1:open", "3:add", "3:open", "10:reduce", "11:close", "13:reduce", "14:open", "15:close"}
	if !slices.Equal(heights, want) {
		t.Fatalf("wallet trades %v, want %v", heights, want)
	}

	// 汇总：alice 在 2 个代币上已实现 -5，bob 在 1 个代币上 +20。
	labels := NewLabelRepo(data)
	clock := time.Now()
	refresher, err := biz.NewRefresher(biz.StatsOptions{
		ChainID: 1, Chain: "eth", Repo: NewStatsRepo(data), Labels: labels, Windows: []int{7, 30}, FastCloseSeconds: 60,
		SmartMoney: biz.SmartMoneyRule{WindowDays: 30, MinRealizedUSD: 10, MinWinRate: 0.5, MinTokens: 1, MaxLossShare: 0.3, MaxWallets: 10},
		Bot:        biz.BotRule{WindowDays: 7, MinTrades: 1000, MinCloses: 1, MinFastCloseShare: 1},
		// 版本是汇总的墙钟时间，wallet_stats 按它过期，所以用真实时间；每次调用加 1 毫秒，
		// 连续的汇总版本不同。
		Now: func() time.Time { clock = clock.Add(time.Millisecond); return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	refresh, ok, err := chdb.LatestWalletRefresh(context.Background(), data.market, 1)
	if err != nil || !ok || refresh.ApplyHeight != 15 {
		t.Fatalf("refresh %+v %v %v", refresh, ok, err)
	}
	var stats []chdb.WalletStatsRow
	if err = data.market.Where("window_days = 30 AND version = ?", refresh.Version).Order("wallet").Find(&stats).Error; err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 {
		t.Fatalf("stats %+v", stats)
	}
	a, b := stats[0].WalletStats(), stats[1].WalletStats()
	if a.Wallet != alice || !a.RealizedUSD.Equal(decimal.RequireFromString("-5")) || a.Tokens != 2 || a.Counted != 3 || a.Wins != 2 ||
		a.Buys != 3 || a.Sells != 3 || a.TokensOver5x != 1 || a.TokensLoss50 != 1 || a.Closes != 1 {
		t.Fatalf("alice stats %+v", a)
	}
	// bob 在 3 买、11 清仓，96 秒，不算快进快出；ROI 2 在 0-2x 桶。
	if b.Wallet != bob || !b.RealizedUSD.Equal(decimal.RequireFromString("20")) || b.WinRate() != 1 || b.Tokens0To2x != 1 ||
		b.HoldSeconds != 96 || b.FastCloses != 0 {
		t.Fatalf("bob stats %+v", b)
	}
	got, err := pgdb.AllWalletLabels(context.Background(), data.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	// alice 15 块 open、15 块 close 是快进快出：bot 规则 1 次清仓、全部快进快出。
	if !slices.Equal(got[bob], []string{model.LabelSmartMoney}) || !slices.Equal(got[alice], []string{model.LabelBot}) {
		t.Fatalf("labels %v", got)
	}

	// 人工标签不受规则影响；不再符合规则的标签被删掉。
	manual := []pgdb.WalletLabelRow{{ChainID: 1, Wallet: alice, Label: "kol", Source: "manual", Since: chain.at(0)}}
	if err = data.state.Create(&manual).Error; err != nil {
		t.Fatal(err)
	}
	if err = labels.ReplaceRuleLabels(context.Background(), 1, []model.WalletLabel{
		{Wallet: bob, Label: model.LabelSmartMoney, Source: model.WalletLabelSource, Since: chain.at(3000)},
	}); err != nil {
		t.Fatal(err)
	}
	got, err = pgdb.AllWalletLabels(context.Background(), data.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got[alice], []string{"kol"}) || !slices.Equal(got[bob], []string{model.LabelSmartMoney}) {
		t.Fatalf("labels after replace %v", got)
	}
	var since []pgdb.WalletLabelRow
	if err = data.state.Where("wallet = ?", bob).Find(&since).Error; err != nil || len(since) != 1 || since[0].Since.Equal(chain.at(3000)) {
		t.Fatalf("bob label should keep its first since: %+v %v", since, err)
	}

	// 每个版本单独保存，发布后只留最新的两个。
	for range 2 {
		if err = refresher.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var versions []uint64
	if err = data.market.Raw("SELECT DISTINCT version FROM wallet_stats ORDER BY version").Scan(&versions).Error; err != nil {
		t.Fatal(err)
	}
	latest, _, err := chdb.LatestWalletRefresh(context.Background(), data.market, 1)
	if err != nil || len(versions) != 2 || versions[1] != latest.Version || versions[0] <= refresh.Version {
		t.Fatalf("versions %v, latest %d, first %d: %v", versions, latest.Version, refresh.Version, err)
	}

	// 回滚删掉流水时记下最早的成交日和保留高度，下次汇总从那天重算。
	repo := NewWalletRepo(data)
	checkpoint, _, err := repo.LoadWalletCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Height != 15 || checkpoint.Part != 0 || checkpoint.Parts != 1 {
		t.Fatalf("checkpoint %+v", checkpoint)
	}
	checkpoint.Height = 14
	if err = repo.RevertWallet(context.Background(), 14, checkpoint); err != nil {
		t.Fatal(err)
	}
	invalidDay, invalidHeight, invalid, err := NewStatsRepo(data).Invalidations(context.Background(), 1, latest.Started)
	if err != nil || !invalid || !invalidDay.Equal(chain.base) || invalidHeight != 14 {
		t.Fatalf("invalidation %v %d %v %v", invalidDay, invalidHeight, invalid, err)
	}
	if err = refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err := NewStatsRepo(data).(*statsRepo).latestStats(context.Background(), alice, 30)
	if err != nil || !profile.RealizedUSD.Equal(decimal.RequireFromString("-50")) {
		t.Fatalf("alice after revert %+v %v", profile, err)
	}

	// 同一高度的后续分片在汇总之后才写入，成交日早于上次汇总重算的范围：下次汇总也要算进去。
	part := []model.WalletTrade{{
		ChainID: 1, ApplyHeight: 14, Part: 1, BlockHeight: 2, BlockTime: chain.base.AddDate(0, 0, -3),
		TransactionHash: "0xpart1", Wallet: alice, Token: tokenX, QuoteToken: weth, PoolID: "0xpool",
		Side: model.SideSell, Event: model.WalletEventReduce, Amount: decimal.RequireFromString("1"),
		ValueUSD: decimal.RequireFromString("100"), RealizedUSD: decimal.RequireFromString("100"), Counted: true,
	}}
	if err = chdb.InsertWalletTrades(context.Background(), data.market, part); err != nil {
		t.Fatal(err)
	}
	if err = refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, err = NewStatsRepo(data).(*statsRepo).latestStats(context.Background(), alice, 30)
	if err != nil || !profile.RealizedUSD.Equal(decimal.RequireFromString("50")) {
		t.Fatalf("alice after a later part of the same height %+v %v", profile, err)
	}
}

// latestStats 读最新一次汇总里一个钱包一个窗口的统计。
func (repo *statsRepo) latestStats(ctx context.Context, wallet string, window int) (model.WalletStats, error) {
	refresh, _, err := chdb.LatestWalletRefresh(ctx, repo.db, 1)
	if err != nil {
		return model.WalletStats{}, err
	}
	var row chdb.WalletStatsRow
	err = repo.db.WithContext(ctx).Where("chain_id = 1 AND version = ? AND wallet = ? AND window_days = ?", refresh.Version, wallet, window).
		Take(&row).Error
	return row.WalletStats(), err
}

// TestTradeSourceFindsTradesHeldLongerThanAnyWindow 补缺按 revision_height 找成交，不限它和
// block_height 相差多少：market-engine 的 backfill_blocks 可以配得很大。
func TestTradeSourceFindsTradesHeldLongerThanAnyWindow(t *testing.T) {
	data := openData(t)
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	late := engineTrade(9600, at, alice, tokenX, "buy", "1", "1")
	late.RevisionHeight = 60000
	rows := []chdb.TradeRow{late, engineTrade(60001, at, alice, tokenX, "buy", "1", "1")}
	if err := chdb.Insert(context.Background(), data.market, &rows); err != nil {
		t.Fatal(err)
	}
	trades, err := NewTradeSource(data).TradesBetween(context.Background(), 1, 60000, 61999)
	if err != nil || len(trades) != 2 || trades[0].BlockHeight != 9600 || trades[0].RevisionHeight != 60000 {
		t.Fatalf("trades %+v %v", trades, err)
	}
	blocks := []chdb.BlockRow{{ChainID: 1, BlockHeight: 59999, BlockTime: at}, {ChainID: 1, BlockHeight: 60000, BlockTime: at.Add(time.Hour)}}
	if err = chdb.Insert(context.Background(), data.market, &blocks); err != nil {
		t.Fatal(err)
	}
	height, ok, err := NewTradeSource(data).LastHeightBefore(context.Background(), 1, 60000, at.Add(time.Minute))
	if err != nil || !ok || height != 59999 {
		t.Fatalf("last height before %d %v %v", height, ok, err)
	}
}

// TestDeleteRecoversPendingInvalidation 删除流水后、写失效记录前崩溃：重启时的删除找不到
// 这些行，也要按存下的范围补写失效记录。
func TestDeleteRecoversPendingInvalidation(t *testing.T) {
	data := openData(t)
	ctx := context.Background()
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if err := data.state.Exec(`INSERT INTO wallet_pending_invalidations (chain_id, day, height) VALUES (1, ?, 7)`, day).Error; err != nil {
		t.Fatal(err)
	}
	if err := NewWalletRepo(data).DiscardTradesAfter(ctx, 1, 9, biz.WholeBlock); err != nil {
		t.Fatal(err)
	}
	invalidDay, invalidHeight, invalid, err := NewStatsRepo(data).Invalidations(ctx, 1, 0)
	if err != nil || !invalid || !invalidDay.Equal(day) || invalidHeight != 7 {
		t.Fatalf("invalidation %v %d %v %v", invalidDay, invalidHeight, invalid, err)
	}
	var left int64
	if err = data.state.Raw(`SELECT count(*) FROM wallet_pending_invalidations`).Scan(&left).Error; err != nil || left != 0 {
		t.Fatalf("pending invalidations left %d %v", left, err)
	}
}
