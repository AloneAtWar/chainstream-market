package biz

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// TestStores open the data layer's stores over a database, for the tests
// that run the processor against real ClickHouse and PostgreSQL. Package
// data imports biz, so only the external test package can build them:
// stores_test.go sets testStores before any test runs.
type TestStores struct {
	// Output writes market-engine's output to a ClickHouse database with
	// the market tables.
	Output func(ctx context.Context, db *gorm.DB) (Output, error)
	// State keeps processor state in a PostgreSQL database.
	State func(db *gorm.DB) StateStore
	// History reads raw trades and prices from a ClickHouse database with
	// the market and raw tables.
	History func(db *gorm.DB, wait time.Duration) History
}

var testStores TestStores

// SetTestStores sets the stores the database tests open.
func SetTestStores(stores TestStores) { testStores = stores }
