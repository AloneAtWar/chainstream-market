package chdb

import (
	"strings"
	"testing"
	"time"
)

func TestRetentionRules(t *testing.T) {
	retention := Retention{
		StoragePolicy: "hot_cold", ColdVolume: "cold", ColdAfterDays: 30, TradesDays: 180,
		TokenPriceRollupDays: 7, CandleCompactDays: 3,
		CandleDays:       map[time.Duration]int{5 * time.Minute: 90, time.Minute: 14, time.Hour: 0},
		RawTransfersDays: 20,
	}
	rules := map[string]string{}
	for _, ttl := range append(retention.EngineTTL(), retention.RawTTL()...) {
		rules[ttl.Table] = strings.Join(ttl.Rules, ", ")
	}
	want := map[string]string{
		"trades": "block_time + toIntervalDay(30) TO VOLUME 'cold', block_time + toIntervalDay(180)",
		"token_prices": "block_time + toIntervalDay(30) TO VOLUME 'cold', block_time + toIntervalDay(7) GROUP BY chain_id, token " +
			"SET block_time = max(block_time), price_usd = argMax(price_usd, block_height), source = argMax(source, block_height), " +
			"hops = argMax(hops, block_height), liquidity_usd = argMax(liquidity_usd, block_height), " +
			"revision_height = argMax(revision_height, block_height), block_height = max(block_height)",
		"candles": "open_time + toIntervalDay(30) TO VOLUME 'cold', open_time + toIntervalDay(3) " +
			"GROUP BY chain_id, subject, key, interval_seconds, open_time SET open = argMax(open, update_height), " +
			"high = argMax(high, update_height), low = argMax(low, update_height), close = argMax(close, update_height), " +
			"volume_base = argMax(volume_base, update_height), volume_usd = argMax(volume_usd, update_height), " +
			"trades = argMax(trades, update_height), update_height = max(update_height), " +
			"open_time + toIntervalDay(14) WHERE interval_seconds = 60, open_time + toIntervalDay(90) WHERE interval_seconds = 300",
		// Deleting sooner than the cold age leaves nothing to move.
		"raw_transfers":  "block_time + toIntervalDay(20)",
		"raw_trades":     "block_time + toIntervalDay(30) TO VOLUME 'cold'",
		"oracle_updates": "block_time + toIntervalDay(30) TO VOLUME 'cold'",
		"asset_facts":    "block_time + toIntervalDay(30) TO VOLUME 'cold'",
		"pool_facts":     "block_time + toIntervalDay(30) TO VOLUME 'cold'",
	}
	for table, rule := range want {
		if rules[table] != rule {
			t.Errorf("%s TTL =\n%s\nwant\n%s", table, rules[table], rule)
		}
	}
	for _, ttl := range (Retention{}).EngineTTL() {
		if len(ttl.Rules) != 0 {
			t.Fatalf("zero retention %s rules = %v", ttl.Table, ttl.Rules)
		}
	}
}

func TestRetentionValidate(t *testing.T) {
	invalid := map[string]Retention{
		"negative":            {TradesDays: -1},
		"negative candles":    {CandleDays: map[time.Duration]int{time.Minute: -1}},
		"fractional interval": {CandleDays: map[time.Duration]int{1500 * time.Millisecond: 1}},
		"policy without cold": {StoragePolicy: "hot_cold", ColdVolume: "cold"},
		"cold without policy": {ColdVolume: "cold", ColdAfterDays: 7},
		"quoted policy":       {StoragePolicy: "a'b", ColdVolume: "cold", ColdAfterDays: 7},
		"quoted volume":       {StoragePolicy: "hot_cold", ColdVolume: "a'b", ColdAfterDays: 7},
	}
	for name, retention := range invalid {
		if err := retention.Validate(); err == nil {
			t.Errorf("%s: Validate() error = nil", name)
		}
	}
	if err := (Retention{}).Validate(); err != nil {
		t.Fatalf("zero Validate() error = %v", err)
	}
}

// outputAt is testOutput moved to blockTime, plus a 1m candle.
