package skiplist

import (
	"kv/pkg/iterator"
	"sync"
)

// SkipListIterator 实现了跳过列表的 Iterator 接口
type SkipListIterator struct {
	current *Node        // 当前跳表访问的节点
	sl      *SkipList    // 迭代器
	txnID   uint64       // 并发控制或版本管理（事务）
	closed  bool         //是否关闭
	mu      sync.RWMutex // 读写锁
}

// NewSkipListIterator 创建一个新的跳跃列表迭代器
func NewSkipListIterator(node *Node, sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		current: node,
		sl:      sl,
		txnID:   0,
		closed:  false,
	}
}

// Valid 如果迭代器指向有效条目，则返回 true
func (iter *SkipListIterator) Valid() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if iter.closed || iter.current == nil {
		return false
	} // 关闭或者

	// 检查当前条目是否对此交易可见 （如果事务id大于迭代器id 事务启动后才被创建 / 修改的。）（事务只能看到比自己小的版本号的数据（避免脏数据））
	if iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		return false
	}
	return true
}

// Key 返回当前条目的键
func (iter *SkipListIterator) Key() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return ""
	}
	return iter.current.Key()
}

// Value 返回当前条目的值
func (iter *SkipListIterator) Value() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return ""
	}
	return iter.current.Value()
}

// TxnID 返回当前条目的事务 ID
func (iter *SkipListIterator) TxnID() uint64 {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return 0
	}
	return iter.current.TxnID()
}

// IsDeleted 如果当前条目是删除标记，则返回 true
func (iter *SkipListIterator) IsDeleted() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return false
	}
	return iter.current.IsDeleted()
}

// Entry 返回当前条目
func (iter *SkipListIterator) Entry() iterator.Entry {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.current.Entry()
}

// Next 将迭代器推进到下一个条目
func (iter *SkipListIterator) Next() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed || iter.current == nil {
		return
	}

	iter.current = iter.current.Next()

	// 跳过此交易不可见的条目
	for iter.current != nil && iter.current != iter.current {
		iter.current = iter.current.Next()
	}
}

// Seek 将迭代器定位到键 >= 目标的第一个条目处
func (iter *SkipListIterator) Seek(key string) bool {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return false
	}

	current := iter.sl.header

	// 导航到目标键或大于目标的第一个键
	for i := iter.sl.level - 1; i >= 0; i-- {
		// 如果有前向指针且，跟前向比小的话往前走
		for current.forward[i] != nil && current.forward[i].CompareKey(key) < 0 {
			current = current.forward[i]
		}
	}
	//第一层的下一个
	iter.current = current.forward[0]

	// 跳过此交易不可见的条目 （大于此迭代其的东西）
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
	// 如果找到完全匹配则返回 true
	if iter.current != nil && iter.current.CompareKey(key) == 0 {
		return true
	}
	return false
}

// SeekToFirst 将迭代器定位到第一个条目
func (iter *SkipListIterator) SeekToFirst() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}
	// 第一层的下一个
	iter.current = iter.sl.header.forward[0]

	// 跳过此交易不可见的条目 （大于此迭代其的东西）
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}

// SeekToLast 将迭代器定位到最后一个条目
func (iter *SkipListIterator) SeekToLast() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	current := iter.sl.header

	//导航到最后一个节点
	for i := iter.sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}

	iter.current = current

	// 如果此条目不可见，则向后移动以找到可见条目
	//注意：这是一个简化的实现。在实践中，你可能需要
	//添加后向指针以实现更高效的后向遍历
	if iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		// 为简单起见，我们只需寻找第一个并迭代到最后一个可见条目
		iter.current = iter.sl.header.forward[0]

		var lastVisible *Node
		for iter.current != nil {
			if iter.txnID == 0 || iter.current.TxnID() <= iter.txnID { // 如果事务可见或者无事务
				lastVisible = iter.current
			}
			iter.current = iter.current.Next()
		}
		iter.current = lastVisible
	}
}

// GetType returns the iterator type
func (iter *SkipListIterator) GetType() iterator.IteratorType {
	return iterator.SkipListIteratorType // 返回0
}

// Close 释放迭代器持有的所有资源
func (iter *SkipListIterator) Close() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.closed = true
	iter.current = nil
	iter.sl = nil
}

// SetTxnID sets the transaction ID for visibility checking
func (iter *SkipListIterator) SetTxnID(txnID uint64) {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.txnID = txnID
}
