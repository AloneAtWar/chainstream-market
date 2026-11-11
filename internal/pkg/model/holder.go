package model

import (
	"math/big"
)

// HolderKey is one holder of one token. Hex addresses are lowercase; TRON
// base58 addresses keep their case.
type HolderKey struct {
	Token  string
	Holder string
}

// Holding is a holder's raw token balance.
type Holding struct {
	HolderKey
	Balance *big.Int
	// AsOf is the block whose post-state Balance already includes: the
	// transfers of blocks up to AsOf are not added again. A balance read at
	// the latest block can be ahead of the block being applied. Negative
	// means the balance must be read again before it is trusted, because the
	// block it was read at was reverted.
	//
	// AsOf 是 Balance 已经包含的区块（该块及以前的转账不再叠加）；在 latest 读取的余额可能
	// 领先于正在应用的块。负数表示读取余额的块已被回滚，须重新读取。
	AsOf int64
}

// AsOfReread is the AsOf of a holding whose balance must be read again.
//
// AsOfReread 表示该持仓的余额须重新读取。
const AsOfReread = -1

// HolderSeed creates a holding first seen in a block. Verified means Balance
// came from balanceOf rather than from summing transfers.
type HolderSeed struct {
	Holding
	Verified bool
}

// HolderDelta adds Delta to an existing holding. Verified marks the result
// as checked against balanceOf at the change's height. A positive AsOf
// replaces the holding's AsOf: the corrected balance was read at that block.
type HolderDelta struct {
	HolderKey
	Delta    *big.Int
	Verified bool
	AsOf     int64
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
