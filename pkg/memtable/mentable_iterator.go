package memtable

import (
	"container/heap"
	"kv/pkg/iterator"
)

// MemTableIterator 迭代 MemTable 中的所有条目（当前表 + 冻结表）
// 它使用最小堆按排序顺序合并来自多个跳表的迭代器
type MemTableIterator struct {
	memTable *MemTable       // 当前迭代器所属的内存表（
	txnID    uint64          // 可见性过滤
	heap     *IteratorHeap   // 堆迭代器（冻结表）
	current  *iterator.Entry // 当前迭代到的条目
	// valid    bool
	closed bool
}

// NewMemTableIterator 为 MemTable 创建一个新的迭代器
func NewMemTableIterator(memTable *MemTable, txnID uint64) *MemTableIterator {
	iter := &MemTableIterator{
		memTable: memTable,
		txnID:    txnID,
		heap:     NewIteratorHeap(),
		closed:   false,
	}

	// 初始化所有表的迭代器
	iter.initializeIterators()
	iter.advance()

	return iter
}

// initializeIterators 为当前表和冻结表设置迭代器
func (iter *MemTableIterator) initializeIterators() {
	// 为当前表添加迭代器
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID) // 当期的活动跳表，生成迭代器
	currentIter.SeekToFirst()

	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0,
		}) // 将currentIter迭代器给heap
	} else {

	}
	iter.memTable.frozenMutex.RUnlock()

	//为冻结表添加迭代器
	iter.memTable.frozenMutex.RLock()
	for i, frozenTable := range iter.memTable.frozenTables {
		frozenIter := frozenTable.NewIterator(iter.txnID)
		frozenIter.SeekToFirst() // 把当前弄到第一个有效
		if frozenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: frozenIter,
				TableID:  i + 1,
			})
		} else {
			frozenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

// advance 移动到下一个有效条目，处理重复项和交易可见性
func (iter *MemTableIterator) advance() {
	iter.current = nil

	if iter.closed || iter.heap.Len() == 0 {
		return
	}

	var lastKey string            // 记录当前处理的键（用于检测重复键）
	var bestEntry *iterator.Entry // 记录当前键的“最佳条目”（最新且可见的版本）
	var bestTxnID uint64          // 记录最佳条目的事务ID（用于比较版本新旧）

	// 处理条目，直到找到具有最高可见交易 ID 的唯一键
	for iter.heap.Len() > 0 {
		item := heap.Pop(iter.heap).(*HeapItem) // 取出堆顶（当前键最小的迭代器条目）
		entry := item.Iterator.Entry()

		//
		if bestEntry == nil || entry.Key != lastKey {
			// 如果有的话，保存之前的最佳条目 ( 且 entry.Key != lastKey)(第二次进入) （如果发现堆顶推进key相同）
			if bestEntry != nil {
				iter.current = bestEntry
				// 放回当前项目，因为它对应不同的键
				heap.Push(iter.heap, item)
				return
			}
			// 首次遇到键就记录最佳
			bestEntry = &entry
			bestTxnID = entry.TxnID
			lastKey = entry.Key
		} else {
			// 相同的密钥 - 检查此版本是否更好（更高的txnID）
			if entry.TxnID > bestTxnID && (iter.txnID == 0 || entry.TxnID <= iter.txnID) {
				bestEntry = &entry
				bestTxnID = entry.TxnID
			}
		}
		// 推进此迭代器
		item.Iterator.Next()
		if item.Iterator.Valid() {
			heap.Push(iter.heap, item)
		} else {
			item.Iterator.Close()
		}
	}

	// Set the final result
	if bestEntry != nil {
		iter.current = bestEntry
	}
}

// Valid 如果迭代器指向有效条目，则返回 true
func (iter *MemTableIterator) Valid() bool {
	return !iter.closed && iter.current != nil
}

// Key returns the key of the current entry
func (iter *MemTableIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Key
}

// Value returns the value of the current entry
func (iter *MemTableIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Value
}

// Entry returns the current entry
func (iter *MemTableIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return *iter.current
}

// Next 接下来将迭代器推进到下一个条目
func (iter *MemTableIterator) Next() {
	if iter.closed {
		return
	}
	iter.advance()
}

// Seek 将迭代器定位到键 >= 目标的第一个条目处
func (iter *MemTableIterator) Seek(key string) bool {
	if iter.closed {
		return false
	}

	// 关闭所有当前迭代器并重新初始化
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()

	// 重新初始化迭代器并寻找目标键(把这个)
	iter.seekToKey(key)
	iter.advance()

	//
	if iter.Valid() && iter.current.Key == key {
		return true
	}
	return false
}

// seekToKey 初始化迭代器并寻找目标键
func (iter *MemTableIterator) seekToKey(key string) {
	//为当前表添加迭代器
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID) // 为这个跳表建一个迭代器
	currentIter.Seek(key)                                             // 跳表到等于key的前一个currunt
	// 如果有效就push进去这个Iter,
	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0,
		})
	} else {
		currentIter.Close()
	}
	iter.memTable.currentMutex.RUnlock()

	// add iterators for frozen table
	iter.memTable.frozenMutex.RLock()
	for i, frozenTable := range iter.memTable.frozenTables {
		frozenIter := frozenTable.NewIterator(iter.txnID)
		frozenIter.Seek(key)
		if frozenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: frozenIter,
				TableID:  i + 1,
			})
		} else {
			frozenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

// SeekToFirst positions the iterator at the first entry
func (iter *MemTableIterator) SeekToFirst() {
	if iter.closed {
		return
	}

	//
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()

	iter.initializeIterators()
	iter.advance()
}

// SeekToLast positions the iterator at the last entry
func (iter *MemTableIterator) SeekToLast() {
	if iter.closed {
		return
	}

	// 为简单起见，我们将先查找第一个元素，然后迭代到最后一个元素
	// 这不是最高效的，但可以正常工作
	iter.SeekToFirst()

	var lastEntry *iterator.Entry
	for iter.Valid() {
		entry := iter.Entry()
		lastEntry = &entry
		iter.Next()
	}

	if lastEntry != nil {
		iter.current = lastEntry
	}
}

// GetType returns the iterator type
func (iter *MemTableIterator) GetType() iterator.IteratorType {
	return iterator.HeapIteratorType
}

// Close releases any resources held by the iterator
func (iter *MemTableIterator) Close() {
	if iter.closed {
		return
	}

	iter.closeAllIterators()
	iter.closed = true
	iter.current = nil
}

// closeAllIterators 关闭堆中的所有迭代器
func (iter *MemTableIterator) closeAllIterators() {
	for iter.heap.Len() > 0 {
		item := heap.Pop(iter.heap).(*HeapItem)
		item.Iterator.Close()
	}
}
