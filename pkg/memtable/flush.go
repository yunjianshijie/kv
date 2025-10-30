package memtable

import (
	"fmt"
	"kv/pkg/config"
	"kv/pkg/iterator"
)

// FlushResult 表示将 MemTable 刷新到存储的结果  用于存储一次刷新（将一个冻结表写入磁盘）的详细信息，方便上层跟踪刷新的数据：
type FlushResult struct {
	Entries       []iterator.Entry //键值对数组
	MinTxnID      uint64           //最小id
	MaxTxnID      uint64           //最大id
	FirstKey      string           //条目中的第一个键
	LastKey       string           //最后一个建
	FlushedSize   int              //数据总大小
	FlushedTxnIDs []uint64         //特殊空条目的事务id（？）
}

// FlushOldest 将最旧的冻结表刷新到存储中
// 如果不存在冻结表或当前表为空，则返回 nil
func (mt *MemTable) FlushOldest() (*FlushResult, error) {
	mt.frozenMutex.Lock()
	defer mt.frozenMutex.Unlock()

	//如果没有冻结表，检查是否需要冻结当前表
	if len(mt.frozenTables) == 0 {
		mt.currentMutex.Lock()
		if mt.currentTable.Size() == 0 {
			mt.currentMutex.Unlock()
			return nil, nil //  Nothing to flush
		}

		// Freeze current table
		mt.freezeCurrentTableLocked()
		mt.currentMutex.Unlock()
	}

	// 获取最旧的冻结表（切片中的最后一个）
	oldestTable := mt.frozenTables[len(mt.frozenTables)-1]

	// 删除
	mt.frozenTables = mt.frozenTables[:len(mt.frozenTables)-1]
	mt.frozenBytes -= oldestTable.SizeBytes()

	// 刷新这个表
	entries := oldestTable.Flush()

	if len(entries) == 0 {
		return &FlushResult{}, nil
	}

	//计算统计数据
	result := &FlushResult{
		Entries:       entries,                     //
		MinTxnID:      entries[0].TxnID,            //
		MaxTxnID:      entries[0].TxnID,            //
		FirstKey:      entries[0].Key,              //
		LastKey:       entries[len(entries)-1].Key, //
		FlushedSize:   oldestTable.SizeBytes(),     //
		FlushedTxnIDs: make([]uint64, 0),           //
	}

	// 查找最小/最大交易 ID 并收集空条目的交易 ID
	for _, entry := range entries {
		//更新max,min
		if entry.TxnID < result.MinTxnID {
			result.MinTxnID = entry.TxnID
		}
		if entry.TxnID > result.MaxTxnID {
			result.MaxTxnID = entry.TxnID
		}
		// 收集空条目的交易 ID（这些可能是特殊标记）
		if entry.Key == "" && entry.Value == "" {
			result.FlushedTxnIDs = append(result.FlushedTxnIDs, entry.TxnID)
		}
	}

	return result, nil
}

// FlushAll 将所有表（当前表和冻结表）刷新到存储中
func (mt *MemTable) FlushAll() ([]*FlushResult, error) {
	var results []*FlushResult

	// 如果当前表有数据，则首先冻结当前表
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()

	if mt.currentTable.Size() > 0 {
		mt.freezeCurrentTableLocked()
	}
	mt.frozenMutex.Unlock()
	mt.currentMutex.Lock()

	// 现在刷新所有冻结的表
	for {
		result, err := mt.FlushOldest()
		if err != nil {
			return results, err
		}
		if result == nil {
			break // 表没用东西
		}
		results = append(results, result)
	}
	return results, nil
}

// CanFlush 如果有表可以刷新，CanFlush 将返回 true
// 只有在真正需要刷新时才应该返回 true，而不仅仅是在有数据时
func (mt *MemTable) CanFlush() bool {
	mt.frozenMutex.RLock()
	hasFrozen := len(mt.frozenTables) > 0 // 是否有冻结表
	mt.frozenMutex.RUnlock()

	// 始终刷新冻结的表，因为它们已经被冻结
	if hasFrozen {
		return true
	}

	// 除非当前表已达到合理大小，否则不要主动刷新当前表
	// 后台工作进程不应立即刷新少量数据
	return false
}

func (mt *MemTable) Empty() bool {
	return (mt.currentTable == nil || mt.currentTable.Size() == 0) && len(mt.frozenTables) == 0
}

// EstimateFlushSize 返回将被刷新的数据的估计大小
func (mt *MemTable) EstimateFlushSize() int {
	mt.frozenMutex.RLock()
	frozenCount := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	if frozenCount > 0 {
		// 如果有冻结表，则返回最旧表的大小
		mt.frozenMutex.RLock()
		oldestSize := mt.frozenTables[len(mt.frozenTables)-1].SizeBytes()
		mt.frozenMutex.RUnlock()
		return oldestSize
	}
	// 返回活动表
	return mt.GetCurrentSize()
}

// GetFlushableTableCount 返回可以刷新的表的数量
func (mt *MemTable) GetFlushableTableCount() int {
	mt.frozenMutex.RLock()
	count := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	mt.currentMutex.RLock()
	if mt.currentTable.Size() > 0 {
		count++
	}
	mt.currentMutex.RUnlock()

	return count
}

// GetMemTableStats 返回有关 MemTable 的统计信息
func (mt *MemTable) GetMemTableStats() MemTableStats {
	mt.currentMutex.RLock()
	currentSize := mt.currentTable.SizeBytes()
	currentEntries := mt.currentTable.Size()
	mt.currentMutex.RUnlock()

	mt.frozenMutex.RLock()
	frozenSize := mt.frozenBytes
	frozenCount := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	return MemTableStats{
		CurrentTableSize:    currentSize,
		CurrentTableEntries: currentEntries,
		FrozenTablesSize:    frozenSize,
		FrozenTablesCount:   frozenCount,
		TotalSize:           currentSize + frozenSize,
		TotalEntries:        currentEntries, // 注意：为了避免复杂性，我们不计算冻结的条目
	}
}

// ShouldFlush 如果 MemTable 应该根据配置刷新，则 ShouldFlush 返回 true
func (mt *MemTable) ShouldFlush() bool {
	stats := mt.GetMemTableStats()

	// 检查是否有太多冻结表
	if stats.FrozenTablesCount > 3 {
		return true
	}

	// 检查总大小是否太大
	totalSizeLimit := mt.getTotalSizeLimit()
	return stats.TotalSize > totalSizeLimit
}

// getTotalSizeLimit 返回所有 MemTables 的总大小限制
func (mt *MemTable) getTotalSizeLimit() int {
	cfg := config.GetGlobalConfig()
	return int(cfg.GetTotalMemSizeLimit())
}

// MemTableStats 表示有关 MemTable 的统计信息
type MemTableStats struct {
	CurrentTableSize    int
	CurrentTableEntries int
	FrozenTablesSize    int
	FrozenTablesCount   int
	TotalSize           int
	TotalEntries        int
}

// String 返回 MemTableStats 的字符串表示
func (stats MemTableStats) String() string {
	return fmt.Sprintf("MemTableStats{Current: %d bytes (%d entries), Frozen: %d bytes (%d tables), Total: %d bytes}",
		stats.CurrentTableSize, stats.CurrentTableEntries,
		stats.FrozenTablesSize, stats.FrozenTablesCount,
		stats.TotalSize)
}
