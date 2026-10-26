package pgdb

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/jackc/pgx/v5/pgtype"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The checkpoint tables of market-engine, holder-indexer and market-registry
// share one layout, CheckpointRow.
const (
	EngineCheckpoints   = "engine_checkpoints"
	HolderCheckpoints   = "holder_checkpoints"
	RegistryCheckpoints = "registry_checkpoints"
)

// CheckpointRow is a row of a checkpoint table; queries name the table.
type CheckpointRow struct {
	ChainID       int64 `gorm:"primaryKey;autoIncrement:false"`
	KafkaOffset   int64
	BlockHeight   int64
	BlockHash     string
	PrunedThrough int64
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
}

// LoadCheckpoint reads chainID's checkpoint from table and the highest
// height whose undo history was pruned.
func LoadCheckpoint(db *gorm.DB, table string, chainID uint64) (chainsync.Checkpoint, int64, bool, error) {
	var row CheckpointRow
	err := db.Table(table).Where("chain_id = ?", int64(chainID)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return chainsync.Checkpoint{}, 0, false, nil
	}
	if err != nil {
		return chainsync.Checkpoint{}, 0, false, fmt.Errorf("postgres store: load %s: %w", table, err)
	}
	return chainsync.Checkpoint{
		ChainID: chainID, Offset: row.KafkaOffset, Height: row.BlockHeight, Hash: row.BlockHash,
		UpdatedAt: row.UpdatedAt.UTC(),
	}, row.PrunedThrough, true, nil
}

// SaveCheckpoint upserts a checkpoint into table. pruned_through never moves
// back.
func SaveCheckpoint(tx *gorm.DB, table string, checkpoint chainsync.Checkpoint, prunedThrough int64) error {
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	row := CheckpointRow{
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

// ArrayParam passes a slice as one PostgreSQL array parameter. GORM expands
// a plain slice into a parenthesized list, which unnest and ANY cannot take.
type ArrayParam[T any] []T

func (values ArrayParam[T]) Value() (driver.Value, error) { return []T(values), nil }

// Array passes values as one array parameter.
func Array[T any](values []T) ArrayParam[T] { return ArrayParam[T](values) }

// pgTypes decodes array columns, which database/sql hands over as text.
var pgTypes = pgtype.NewMap()

// StringArray scans a TEXT[] column.
type StringArray []string

func (values *StringArray) Scan(src any) error {
	var decoded []string
	if err := pgTypes.SQLScanner(&decoded).Scan(src); err != nil {
		return fmt.Errorf("scan text array: %w", err)
	}
	*values = decoded
	return nil
}

func (values StringArray) Value() (driver.Value, error) { return []string(values), nil }

func (StringArray) GormDataType() string { return "text[]" }

// Runner runs statements in a transaction until the first error, which it
// keeps in Err.
type Runner struct {
	Tx  *gorm.DB
	Err error
}

// Exec runs statement unless an earlier one failed.
func (run *Runner) Exec(statement string, values ...any) {
	if run.Err == nil {
		run.Err = run.Tx.Exec(statement, values...).Error
	}
}

// JSONText returns raw as a JSONB parameter, or nil when it is empty.
func JSONText(raw []byte) *string {
	if len(raw) == 0 {
		return nil
	}
	text := string(raw)
	return &text
}
