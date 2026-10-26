package pgstore

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
