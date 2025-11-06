package skiplist

import (
	"math/rand"
	"time"

	"kv/pkg/iterator"
)

const (
	// MaxLevel
	MaxLevel = 16
	// Probability
	Probability = 0.5
)

// SkipList represents a skip list data structure
// SkipList 表示跳过列表数据结构
type SkipList struct {
	header    *Node      // header node (sentinel) // 头节点（哨兵节点，不存储实际数据，作为查询起点）
	level     int        // current maximum level  // 最大层数
	size      int        // number of elements 		// number元素总数
	sizeBytes int        // approximate memory size in bytes // 估算的内存占用字节数
	rng       *rand.Rand // random number generator	// 随机数生成器，用于插入时决定新节点的层数
}

// New creates a new skip list
func New() *SkipList {
	header := NewNode("", "", 0, MaxLevel)
	return &SkipList{
		header:    header,
		level:     1,
		size:      0,
		sizeBytes: 0,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < MaxLevel && sl.rng.Float64() < Probability {
		level++
	}
	return level
}

// Delete marks a key as deleted (tombstone)
func (sl *SkipList) Delete(key string, txnID uint64) {
	sl.Put(key, "", txnID)
}

// Put 是插入/更新条目的内部方法
func (sl *SkipList) Put(key, value string, txnID uint64) {
	update := make([]*Node, MaxLevel) //
	current := sl.header

	// 临时节点
	tmpNode := NewNode(key, value, txnID, 1)
	// Find the insertion point 找到插入点
	for i := sl.level - 1; i >= 0; i-- { // 找每一层
		for current.forward[i] != nil && current.forward[i].CompareNode(tmpNode) < 0 { // 找到当前层的范围
			current = current.forward[i] //下一层
		}
		update[i] = current
	}
	// 当前等于存的节点下一个
	// If key already exists, we need to handle MVCC properly
	// In LSM-tree, we always insert new versions
	// 如果键已经存在，我们需要正确处理 MVCC
	// 在 LSM 树中，我们总是插入新版本
	level := sl.randomLevel()
	//如果最高树
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	newNode := NewNode(key, value, txnID, level)

	// Set forward pointers and update backward pointers 设置前向指针并更新后向指针
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		if update[i].forward[i] != nil {
			update[i].forward[i].backward = newNode
		}
		update[i].forward[i] = newNode
	}

	// Set backward pointer 设置后向指针
	newNode.backward = update[0]
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	}

	sl.size++
	sl.sizeBytes += len(key) + len(value) + 8 // approximate size calculation
}

// Get 查找给定交易可见的密钥的最新版本(给一个迭代器)
func (sl *SkipList) Get(key string, txnID uint64) *SkipListIterator {
	current := sl.header

	// 找到新的key
	tempNode := NewNode(key, "", txnID, 1)
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].CompareNode(tempNode) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	// 查找此transaction可见的第一个版本
	for current != nil && current.Key() == key {
		if txnID == 0 || current.TxnID() <= txnID {
			//
			iter := NewSkipListIterator(current, sl)
			iter.SetTxnID(txnID)
			return iter
		}
		current = current.forward[0]
	}
	// Key not found or no visible version （如果没有找到这个key）
	iter := NewSkipListIterator(nil, sl)
	iter.SetTxnID(txnID)
	return iter
}

// Size returns the number of entries in the skip list
func (sl *SkipList) Size() int {
	return sl.size
}

// SizeBytes returns the approximate memory usage in bytes
func (sl *SkipList) SizeBytes() int {

	return sl.sizeBytes
}

// IsEmpty 如果跳过列表为空，则返回 true
func (sl *SkipList) IsEmpty() bool {
	return sl.size == 0
}

// Clear removes all entries from the skip list
func (sl *SkipList) Clear() {
	sl.header = NewNode("", "", 0, MaxLevel)
	sl.level = 1
	sl.size = 0
	sl.sizeBytes = 0
}

// NewIterator 为跳表创建一个新的迭代器
func (sl *SkipList) NewIterator(txnID uint64) iterator.Iterator {

	iter := NewSkipListIterator(sl.header.forward[0], sl)
	iter.SetTxnID(txnID)
	return iter
}

// Flush 将跳过列表中的所有条目作为切片返回    将跳表中存储的所有键值对（及元信息）提取为一个有序的条目切片
func (sl *SkipList) Flush() []iterator.Entry {

	entries := make([]iterator.Entry, 0, sl.size)
	current := sl.header.forward[0]

	for current != nil {
		entries = append(entries, current.Entry())
		current = current.forward[0]
	}

	return entries
}
