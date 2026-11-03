package data

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/AloneAtWar/chainstream-market/internal/app/launchpad/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/pgdb"
	"gorm.io/gorm"
)

// launchpadRepo keeps the launchpad tables of the PostgreSQL database.
type launchpadRepo struct {
	db *gorm.DB
}

// NewLaunchpadRepo returns market-launchpad's repository.
//
// NewLaunchpadRepo 返回 market-launchpad 的 PostgreSQL 存储。
func NewLaunchpadRepo(db *gorm.DB) biz.LaunchpadRepo { return &launchpadRepo{db: db} }

// LoadLaunchpadCheckpoint returns the last committed checkpoint and the
// highest height whose undo history was pruned.
func (repo *launchpadRepo) LoadLaunchpadCheckpoint(ctx context.Context, chainID uint64) (chainsync.Checkpoint, int64, bool, error) {
	return pgdb.LoadCheckpoint(repo.db.WithContext(ctx), pgdb.LaunchpadCheckpoints, chainID)
}

// CommitLaunchpad applies change and saves checkpoint in one transaction.
func (repo *launchpadRepo) CommitLaunchpad(ctx context.Context, checkpoint chainsync.Checkpoint, change model.LaunchpadChange) error {
	if checkpoint.ChainID == 0 {
		return errors.New("postgres store: checkpoint chain id is required")
	}
	chainID := int64(checkpoint.ChainID)
	err := repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := &pgdb.Runner{Tx: tx}
		switch {
		case change.Reverted:
			queueRevert(run, chainID, change.RevertTo)
		case change.Applied:
			queueTokens(run, chainID, change)
			run.Exec(`INSERT INTO launchpad_heads (chain_id, block_height, block_time) VALUES (?, ?, ?)
				ON CONFLICT (chain_id) DO UPDATE SET block_height = EXCLUDED.block_height, block_time = EXCLUDED.block_time`,
				chainID, change.Height, change.Time)
		}
		run.Exec(`DELETE FROM launchpad_undo WHERE chain_id = ? AND block_height <= ?`, chainID, change.PrunedThrough)
		if run.Err != nil {
			return run.Err
		}
		return pgdb.SaveCheckpoint(tx, pgdb.LaunchpadCheckpoints, checkpoint, change.PrunedThrough)
	})
	if err != nil {
		return fmt.Errorf("postgres store: commit launchpad chain %d offset %d: %w", checkpoint.ChainID, checkpoint.Offset, err)
	}
	return nil
}

// queueRevert puts every token the reverted blocks touched back as it was
// before the first of them: the row is deleted, then the oldest undo image
// above rollbackTo is restored unless the row did not exist yet. The head
// keeps its block time until the replacement branch's first block moves it,
// so the list windows stay at most one reorg's depth ahead meanwhile.
func queueRevert(run *pgdb.Runner, chainID, rollbackTo int64) {
	values := map[string]any{"chain": chainID, "to": rollbackTo}
	run.Exec(`DELETE FROM launchpad_tokens AS t USING launchpad_undo AS u
		WHERE u.chain_id = @chain AND u.block_height > @to AND t.chain_id = u.chain_id AND t.token = u.token`, values)
	run.Exec(`INSERT INTO launchpad_tokens
		SELECT (jsonb_populate_record(NULL::launchpad_tokens, u.prev)).*
		FROM (
			SELECT DISTINCT ON (token) prev FROM launchpad_undo
			WHERE chain_id = @chain AND block_height > @to
			ORDER BY token, block_height
		) AS u
		WHERE u.prev IS NOT NULL`, values)
	run.Exec(`DELETE FROM launchpad_undo WHERE chain_id = @chain AND block_height > @to`, values)
	run.Exec(`UPDATE launchpad_heads SET block_height = @to WHERE chain_id = @chain AND block_height > @to`, values)
}

// queueTokens records the touched rows for undo, then merges the block's
// change into each: launch details fill in once, the status only moves
// forward, the curve state follows the latest trade and the counts add up.
func queueTokens(run *pgdb.Runner, chainID int64, change model.LaunchpadChange) {
	if len(change.Tokens) == 0 {
		return
	}
	count := len(change.Tokens)
	tokens, protocols, pools, managers, quotes, statuses := make([]string, count), make([]string, count),
		make([]string, count), make([]string, count), make([]string, count), make([]string, count)
	creators, names, symbols, metaURIs, launchTxs, targets := make([]*string, count), make([]*string, count),
		make([]*string, count), make([]*string, count), make([]*string, count), make([]*string, count)
	launched := make([]bool, count)
	progress, remaining, raised, prices := make([]*string, count), make([]*string, count), make([]*string, count), make([]*string, count)
	trades, buys, sells := make([]int64, count), make([]int64, count), make([]int64, count)
	lastTrade := make([]*int64, count)
	for index, token := range change.Tokens {
		tokens[index], protocols[index], pools[index] = token.Token, token.Protocol, token.PoolID
		managers[index], quotes[index], statuses[index] = token.Manager, token.Quote, token.Status
		creators[index], names[index], symbols[index] = optional(token.Creator), optional(token.Name), optional(token.Symbol)
		metaURIs[index], launchTxs[index], targets[index] = optional(token.MetaURI), optional(token.LaunchTx), optional(token.TargetPoolID)
		launched[index] = token.Launched
		progress[index], remaining[index] = bigText(token.Progress), bigText(token.BaseRemaining)
		raised[index], prices[index] = bigText(token.QuoteRaised), bigText(token.Price)
		trades[index], buys[index], sells[index] = token.Trades, token.Buys, token.Sells
		if token.LastTradeTime != 0 {
			value := token.LastTradeTime
			lastTrade[index] = &value
		}
	}
	run.Exec(`INSERT INTO launchpad_undo (chain_id, block_height, token, prev)
		SELECT @chain, @height, k.token, to_jsonb(t)
		FROM unnest(CAST(@tokens AS text[])) AS k(token)
		LEFT JOIN launchpad_tokens AS t ON t.chain_id = @chain AND t.token = k.token
		ON CONFLICT DO NOTHING`, map[string]any{
		"chain": chainID, "height": change.Height, "tokens": pgdb.Array(slices.Sorted(slices.Values(tokens))),
	})
	run.Exec(`INSERT INTO launchpad_tokens AS t (chain_id, token, protocol, pool_id, manager, quote, creator, name,
			symbol, meta_uri, launch_tx, launch_seen, launch_height, launch_time, status, progress, base_remaining,
			quote_raised, price, trades, buys, sells, last_trade_time, completed_height, completed_time,
			migrated_height, migrated_time, target_pool_id, updated_height)
		SELECT @chain, f.token, f.protocol, f.pool_id, f.manager, f.quote, f.creator, f.name, f.symbol, f.meta_uri,
			f.launch_tx, f.launched, @height, @time, f.status, f.progress::numeric, f.base_remaining::numeric,
			f.quote_raised::numeric, f.price::numeric, f.trades, f.buys, f.sells, f.last_trade_time,
			CASE WHEN f.status <> 'bonding' THEN CAST(@height AS bigint) END,
			CASE WHEN f.status <> 'bonding' THEN CAST(@time AS bigint) END,
			CASE WHEN f.status = 'migrated' THEN CAST(@height AS bigint) END,
			CASE WHEN f.status = 'migrated' THEN CAST(@time AS bigint) END,
			f.target_pool_id, @height
		FROM unnest(CAST(@tokens AS text[]), CAST(@protocols AS text[]), CAST(@pools AS text[]), CAST(@managers AS text[]),
			CAST(@quotes AS text[]), CAST(@creators AS text[]), CAST(@names AS text[]), CAST(@symbols AS text[]),
			CAST(@meta_uris AS text[]), CAST(@launch_txs AS text[]), CAST(@launched AS bool[]), CAST(@statuses AS text[]),
			CAST(@progress AS text[]), CAST(@remaining AS text[]), CAST(@raised AS text[]), CAST(@prices AS text[]),
			CAST(@trades AS bigint[]), CAST(@buys AS bigint[]), CAST(@sells AS bigint[]), CAST(@last_trade AS bigint[]),
			CAST(@targets AS text[]))
			AS f(token, protocol, pool_id, manager, quote, creator, name, symbol, meta_uri, launch_tx, launched, status,
				progress, base_remaining, quote_raised, price, trades, buys, sells, last_trade_time, target_pool_id)
		ON CONFLICT (chain_id, token) DO UPDATE SET
			creator = COALESCE(EXCLUDED.creator, t.creator), name = COALESCE(EXCLUDED.name, t.name),
			symbol = COALESCE(EXCLUDED.symbol, t.symbol), meta_uri = COALESCE(EXCLUDED.meta_uri, t.meta_uri),
			launch_tx = COALESCE(EXCLUDED.launch_tx, t.launch_tx),
			launch_height = CASE WHEN EXCLUDED.launch_seen AND NOT t.launch_seen THEN EXCLUDED.launch_height ELSE t.launch_height END,
			launch_time = CASE WHEN EXCLUDED.launch_seen AND NOT t.launch_seen THEN EXCLUDED.launch_time ELSE t.launch_time END,
			launch_seen = t.launch_seen OR EXCLUDED.launch_seen,
			status = CASE WHEN array_position(@ranks, EXCLUDED.status) > array_position(@ranks, t.status)
				THEN EXCLUDED.status ELSE t.status END,
			progress = COALESCE(EXCLUDED.progress, t.progress),
			base_remaining = COALESCE(EXCLUDED.base_remaining, t.base_remaining),
			quote_raised = COALESCE(EXCLUDED.quote_raised, t.quote_raised), price = COALESCE(EXCLUDED.price, t.price),
			trades = t.trades + EXCLUDED.trades, buys = t.buys + EXCLUDED.buys, sells = t.sells + EXCLUDED.sells,
			last_trade_time = COALESCE(EXCLUDED.last_trade_time, t.last_trade_time),
			completed_height = COALESCE(t.completed_height, EXCLUDED.completed_height),
			completed_time = COALESCE(t.completed_time, EXCLUDED.completed_time),
			migrated_height = COALESCE(t.migrated_height, EXCLUDED.migrated_height),
			migrated_time = COALESCE(t.migrated_time, EXCLUDED.migrated_time),
			target_pool_id = COALESCE(EXCLUDED.target_pool_id, t.target_pool_id),
			updated_height = EXCLUDED.updated_height`, map[string]any{
		"chain": chainID, "height": change.Height, "time": change.Time,
		"ranks":  pgdb.Array([]string{model.LaunchpadBonding, model.LaunchpadCompleted, model.LaunchpadMigrated}),
		"tokens": pgdb.Array(tokens), "protocols": pgdb.Array(protocols), "pools": pgdb.Array(pools),
		"managers": pgdb.Array(managers), "quotes": pgdb.Array(quotes), "creators": pgdb.Array(creators),
		"names": pgdb.Array(names), "symbols": pgdb.Array(symbols), "meta_uris": pgdb.Array(metaURIs),
		"launch_txs": pgdb.Array(launchTxs), "launched": pgdb.Array(launched), "statuses": pgdb.Array(statuses),
		"progress": pgdb.Array(progress), "remaining": pgdb.Array(remaining), "raised": pgdb.Array(raised),
		"prices": pgdb.Array(prices), "trades": pgdb.Array(trades), "buys": pgdb.Array(buys), "sells": pgdb.Array(sells),
		"last_trade": pgdb.Array(lastTrade), "targets": pgdb.Array(targets),
	})
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func bigText(value *big.Int) *string {
	if value == nil {
		return nil
	}
	text := value.String()
	return &text
}
