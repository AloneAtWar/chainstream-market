package chainsync

// PruneThrough is the highest height whose undo history can be dropped after
// applying the block at height. A revert can never reach the finalized
// height, and history_depth is the deepest reorg a service can undo, so the
// later of the two bounds wins: at most depth blocks of history are kept,
// fewer when the finalized height is closer to the head. An unknown
// finalized height (0) leaves only the depth. Services keep the result
// monotonic and reject a revert below it with ErrReorgTooDeep.
//
// PruneThrough 返回应用 height 区块后可以丢弃撤销历史的最高高度。回滚不会越过
// finalized 高度，history_depth 是服务能撤销的最大重组深度，所以取两条边界中更晚
// 的一个：max(finalized, height−depth)。撤销历史最多保留 depth 个区块，finalized
// 更接近链头时保留得更少；finalized 未知（0）时只按 depth 裁剪。各服务保持该值单调
// 不减，回滚高度低于它时返回 ErrReorgTooDeep。history_depth 不应小于上游
// chainstream-evm 的 scanner.cache_size，否则上游能发出的 revert 可能无法撤销。
func PruneThrough(height, finalized, depth int64) int64 {
	return max(finalized, height-depth)
}
