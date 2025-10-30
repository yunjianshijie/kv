package memtable

import (
	"kv/pkg/common"
	"kv/pkg/config"
	"kv/pkg/iterator"
	"kv/pkg/skiplist"
	"sync"
)

// MemTable 表示 LSM 树的内存表组件
// 它管理当前（活动）和冻结（不可变）的跳过列表表
type MemTable struct {
	//  当前接受写入的活动表
	currentTable *skiplist.SkipList
	// 冻结的表是不可变的，等待刷新
	frozenTables []*skiplist.SkipList
	//
	frozenBytes int // Total bytes in frozen tables 冻结表中的总字节数
	// Mutexes for thread safety
	currentMutex sync.RWMutex // Protects current table
	frozenMutex  sync.RWMutex // Protects frozen tables
}

// New 创建新的 MemTable 实例
func New() *MemTable {
	return &MemTable{
		currentTable: skiplist.New(),
		frozenTables: make([]*skiplist.SkipList, 0),
		frozenBytes:  0,
	}
}

// Put 将键值对添加到当前活动表
func (mt *MemTable) Put(key, value string, txnID uint64) error {

	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// 加current table(current table更新词条)
	mt.currentTable.Put(key, value, txnID)

	//检查当前表大小是否超出限制并需要冻结
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		//需要冻结当前表
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}
	return nil
}

// PutBatch 在单个操作中添加多个键值对
func (mt *MemTable) PutBatch(kvs []common.KVPair, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	//将所有条目添加到当前表
	for _, kv := range kvs {
		mt.currentTable.Put(kv.Key, kv.Value, txnID)
	}

	//检查当前表大小是否超出限制并需要冻结
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}
	return nil
}

// Delete 删除时，通过放置空值（墓碑）将键标记为已删除
func (mt *MemTable) Delete(key string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	//通过输入空值（墓碑）来删除
	mt.currentTable.Delete(key, txnID)

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// DeleteBatch marks multiple keys as deleted in a single operation
func (mt *MemTable) DeleteBatch(keys []string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Delete all keys by putting empty values (tombstones)
	for _, key := range keys {
		mt.currentTable.Delete(key, txnID)
	}

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// Get 从 MemTable 中检索一个键，首先检查当前表，然后检查冻结表
func (mt *MemTable) Get(key string, txnID uint64) (string, bool, error) {
	// 检查当前表
	mt.currentMutex.RLock()
	iter := mt.currentTable.Get(key, txnID)
	mt.currentMutex.RUnlock()

	if iter.Valid() {
		value := iter.Value()
		isDeleted := iter.IsDeleted()
		iter.Close()

		if isDeleted {
			return "", false, nil // Key was deleted
		}
		return value, true, nil
	}
	iter.Close()

	// 如果在当前表中找不到，则检查冻结表
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	// 从最新到最旧检查冻结的表
	for i := 0; i < len(mt.frozenTables); i++ {
		iter := mt.frozenTables[i].Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			iter.Close()

			if isDeleted {
				return "", false, nil
			}
			return value, true, nil
		}
		iter.Close()
	}
	return "", false, nil // Key not found
}

// GetBatch 从 MemTable 中检索多个键
func (mt *MemTable) GetBatch(keys []string, txnID uint64) ([]GetResult, error) {
	results := make([]GetResult, len(keys))

	// First check current table for all keys
	mt.currentMutex.RLock()
	for i, key := range keys {
		iter := mt.currentTable.Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			results[i] = GetResult{
				Key:   key,
				Value: value,
				Found: !isDeleted,
			}
		} else {
			results[i] = GetResult{
				Key:   key,
				Found: false,
			}
		}
		iter.Close()
	}
	mt.currentMutex.RUnlock()

	// For keys not found in current table, check frozen tables
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	for i, key := range keys {
		if results[i].Found {
			continue // Already found in current table
		}

		// Check frozen tables from newest to oldest
		for j := 0; j < len(mt.frozenTables); j++ {
			iter := mt.frozenTables[j].Get(key, txnID)
			if iter.Valid() {
				value := iter.Value()
				isDeleted := iter.IsDeleted()
				results[i] = GetResult{
					Key:   key,
					Value: value,
					Found: !isDeleted,
				}
				iter.Close()
				break
			}
			iter.Close()
		}
	}

	return results, nil
}

// freezeCurrentTableLocked 冻结当前表并创建一个新表
// 调用者必须同时持有 currentMutex 和 frozenMutex 锁
func (mt *MemTable) freezeCurrentTableLocked() {
	// 将当前表移动到冻结表列表的前面
	mt.frozenBytes += mt.currentTable.SizeBytes()

	// 添加到切片的开头（最新冻结的表优先）
	mt.frozenTables = append([]*skiplist.SkipList{mt.currentTable}, mt.frozenTables...)

	// 创建新的当前表
	mt.currentTable = skiplist.New()
}

// FreezeCurrentTable 明确冻结当前表
func (mt *MemTable) FreezeCurrentTable() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	mt.freezeCurrentTableLocked()
	mt.frozenMutex.Unlock()
	mt.currentMutex.Unlock()
}

// GetCurrentSize 返回当前活动表的大小
func (mt *MemTable) GetCurrentSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.SizeBytes()
}

// GetFrozenSize 返回所有冻结表的总大小
func (mt *MemTable) GetFrozenSize() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return mt.frozenBytes
}

// GetTotalSize returns the total size of current and frozen tables
func (mt *MemTable) GetTotalSize() int {
	return mt.GetCurrentSize() + mt.GetFrozenSize()
}

// GetCurrentTableSize 返回当前表中的条目数
func (mt *MemTable) GetCurrentTableSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.Size()
}

// GetFrozenTableCount returns the number of frozen tables
func (mt *MemTable) GetFrozenTableCount() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return len(mt.frozenTables)
}

// Clear removes all data from the MemTable
func (mt *MemTable) Clear() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	defer mt.currentMutex.Unlock()
	defer mt.frozenMutex.Unlock()

	mt.currentTable.Clear()
	mt.frozenTables = make([]*skiplist.SkipList, 0)
	mt.frozenBytes = 0
}

// NewIterator 创建一个新的迭代器，迭代 MemTable 中的所有条目
func (mt *MemTable) NewIterator(txnID uint64) iterator.Iterator {
	return NewMemTableIterator(mt, txnID)
}

// GetResult  表示 Get 操作的结果
type GetResult struct {
	Key   string
	Value string
	Found bool
}
