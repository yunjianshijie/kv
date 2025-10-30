package memtable

import (
	"container/heap"
	"kv/pkg/iterator"
)

// HeapItem 表示迭代器堆中的一个项目
type HeapItem struct {
	Iterator iterator.Iterator // 迭代器（）
	TableID  int               // 0 for current table, 1+ for frozen tables   0当前活跃的内存表 1+冻结表
}

// IteratorHeap 实现按当前键排序的迭代器最小堆
type IteratorHeap []*HeapItem

// NewIteratorHeap  creates a new iterator heap
func NewIteratorHeap() *IteratorHeap {
	h := make(IteratorHeap, 0)
	heap.Init(&h)
	return &h
}

// Len returns the number of items in the heap
func (h IteratorHeap) Len() int { return len(h) }

// Less 比较两个堆项
func (h IteratorHeap) Less(i, j int) bool {
	keyI := h[i].Iterator.Key()
	keyJ := h[j].Iterator.Key()

	if keyI != keyJ {
		return keyI < keyJ
	}

	// 相同键 - 按事务 ID 排序（优先级高），然后是表 ID（对于较新的表，优先级低）
	txnI := h[i].Iterator.TxnID()
	txnJ := h[j].Iterator.TxnID()

	if txnI != txnJ {
		return txnI < txnJ
	}
	//
	return h[i].TableID < h[j].TableID //// 较低的表 ID（较新）优先
}

// Swap 交换堆中的两个元素
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an item to the heap
func (h *IteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapItem))
}

// Pop 从堆中移除并返回最小项
func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1] // 把最后一个去掉
	return item       // 返回最后一个
}
