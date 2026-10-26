package pgstore

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
	"github.com/jackc/pgx/v5/pgtype"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The checkpoint tables of market-engine, holder-indexer and market-registry
// share one layout, checkpointRow.
const (
	engineCheckpoints   = "engine_checkpoints"
	holderCheckpoints   = "holder_checkpoints"
	registryCheckpoints = "registry_checkpoints"
)

// checkpointRow is a row of a checkpoint table; queries name the table.
type checkpointRow struct {
	ChainID       int64 `gorm:"primaryKey;autoIncrement:false"`
	KafkaOffset   int64
	BlockHeight   int64
	BlockHash     string
	PrunedThrough int64
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
}

// stateRow is an engine_state row read with value as text.
type stateRow struct {
	ChainID int64 `gorm:"primaryKey;autoIncrement:false"`
	Kind    string
	Key     string
	Value   string
}

func (stateRow) TableName() string { return "engine_state" }

// stateUndoRow is an engine_state_undo row read with prev as text.
type stateUndoRow struct {
	ChainID     int64 `gorm:"primaryKey;autoIncrement:false"`
	BlockHeight int64
	Kind        string
	Key         string
	Prev        *string
}

func (stateUndoRow) TableName() string { return "engine_state_undo" }

// holdingRow is a holder_balances row read with balance as text.
type holdingRow struct {
	Token   string
	Holder  string
	Balance string
}

// holderCountRow is a holder_counts row.
type holderCountRow struct {
	ChainID int64 `gorm:"primaryKey;autoIncrement:false"`
	Token   string
	Holders int64
}

func (holderCountRow) TableName() string { return "holder_counts" }

// rankingCheckpointRow is a ranking_checkpoints row.
type rankingCheckpointRow struct {
	ChainID     int64 `gorm:"primaryKey;autoIncrement:false"`
	HeadTime    time.Time
	RefreshedAt time.Time
}

func (rankingCheckpointRow) TableName() string { return "ranking_checkpoints" }

// tokenRow is a registry_tokens row read with its JSONB columns as text.
type tokenRow struct {
	Address          string
	Standard         string
	Recognition      string
	MetadataComplete bool
	Name             *string
	Symbol           *string
	Decimals         *int16
	Metadata         *string
	Created          bool
	Deployment       *string
	SourceHeight     int64
	FirstHeight      int64
	UpdatedHeight    int64
}

func (tokenRow) TableName() string { return "registry_tokens" }

// poolRow is a registry_pools row read with metadata as text.
type poolRow struct {
	PoolID        string
	Protocol      string
	DexAddress    string
	Tokens        stringArray
	Created       bool
	SourceHeight  int64
	SourceTime    int64
	Metadata      *string
	FirstHeight   int64
	UpdatedHeight int64
}

func (poolRow) TableName() string { return "registry_pools" }

// arrayParam passes a slice as one PostgreSQL array parameter. GORM expands
// a plain slice into a parenthesized list, which unnest and ANY cannot take.
type arrayParam[T any] []T

func (values arrayParam[T]) Value() (driver.Value, error) { return []T(values), nil }

func array[T any](values []T) arrayParam[T] { return arrayParam[T](values) }

// pgTypes decodes array columns, which database/sql hands over as text.
var pgTypes = pgtype.NewMap()

// stringArray scans a TEXT[] column.
type stringArray []string

func (values *stringArray) Scan(src any) error {
	var decoded []string
	if err := pgTypes.SQLScanner(&decoded).Scan(src); err != nil {
		return fmt.Errorf("scan text array: %w", err)
	}
	*values = decoded
	return nil
}

func (values stringArray) Value() (driver.Value, error) { return []string(values), nil }

func (stringArray) GormDataType() string { return "text[]" }

// runner runs statements in a transaction until the first error, which it
// keeps.
type runner struct {
	tx  *gorm.DB
	err error
}

func (run *runner) exec(statement string, values ...any) {
	if run.err == nil {
		run.err = run.tx.Exec(statement, values...).Error
	}
}

func loadCheckpoint(db *gorm.DB, table string, chainID uint64) (storage.Checkpoint, int64, bool, error) {
	var row checkpointRow
	err := db.Table(table).Where("chain_id = ?", int64(chainID)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return storage.Checkpoint{}, 0, false, nil
	}
	if err != nil {
		return storage.Checkpoint{}, 0, false, fmt.Errorf("postgres store: load %s: %w", table, err)
	}
	return storage.Checkpoint{
		ChainID: chainID, Offset: row.KafkaOffset, Height: row.BlockHeight, Hash: row.BlockHash,
		UpdatedAt: row.UpdatedAt.UTC(),
	}, row.PrunedThrough, true, nil
}

// saveCheckpoint upserts a checkpoint. pruned_through never moves back.
func saveCheckpoint(tx *gorm.DB, table string, checkpoint storage.Checkpoint, prunedThrough int64) error {
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	row := checkpointRow{
		ChainID: int64(checkpoint.ChainID), KafkaOffset: checkpoint.Offset, BlockHeight: checkpoint.Height,
		BlockHash: checkpoint.Hash, PrunedThrough: prunedThrough, UpdatedAt: updatedAt,
	}
	return tx.Table(table).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "chain_id"}},
		DoUpdates: append(clause.AssignmentColumns([]string{"kafka_offset", "block_height", "block_hash", "updated_at"}),
			clause.Assignment{
				Column: clause.Column{Name: "pruned_through"},
				Value:  gorm.Expr("GREATEST(" + table + ".pruned_through, EXCLUDED.pruned_through)"),
			}),
	}).Create(&row).Error
}
