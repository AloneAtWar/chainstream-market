package model

import (
	"math/big"
)

// HolderKey is one holder of one token. Both addresses are lowercase.
type HolderKey struct {
	Token  string
	Holder string
}

// Holding is a holder's raw token balance.
type Holding struct {
	HolderKey
	Balance *big.Int
}

// HolderSeed creates a holding first seen in a block. Verified means Balance
// came from balanceOf rather than from summing transfers.
type HolderSeed struct {
	Holding
	Verified bool
}

// HolderDelta adds Delta to an existing holding. Verified marks the result
// as checked against balanceOf at the change's height.
type HolderDelta struct {
	HolderKey
	Delta    *big.Int
	Verified bool
}

// HolderChange is what one record changed in the holder balances.
type HolderChange struct {
	// Reverted undoes every change above RevertTo before anything else.
	Reverted bool
	RevertTo int64
	// Height is the block that Created and Deltas belong to.
	Height  int64
	Created []HolderSeed
	Deltas  []HolderDelta
	// PrunedThrough discards undo history at or below it.
	PrunedThrough int64
}
