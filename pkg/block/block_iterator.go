package block

import (
	"kv/pkg/iterator"
)

// BlockIterator 实现了支持 MVCC 的块的 Iterator 接口
type BlockIterator struct {
	block      *Block           // 块
	index      int              //
	txnID      uint64           //
	closed     bool             // 是否关闭
	aggregated []iterator.Entry // Pre-aggregated MVCC entries 预聚合 MVCC 条目
}

// NewBlockIterator 创建一个新的块迭代器
func NewBlockIterator(block *Block) *BlockIterator {
	iter := &BlockIterator{
		block:  block,
		index:  -1,
		txnID:  0,
		closed: false,
	}

	// Pre-aggregate MVCC entries
	iter.buildAggregatedEntries()

	return iter
}

// buildAggregatedEntries 按键预先聚合 MVCC 的所有条目
func (iter *BlockIterator) buildAggregatedEntries() {
	// 如果块里面条目为0
	if iter.block.NumEntries() == 0 {
		iter.aggregated = []iterator.Entry{}
		return
	}

	// 按键对条目进行分组，并根据 txnID 为每个键保留最佳版本
	keyMap := make(map[string]*iterator.Entry)

	for i := 0; i < iter.block.NumEntries(); i++ {
		entry, err := iter.block.GetEntry(i)
		if err != nil {
			continue
		}

		//检查此条目是否对我们的交易可见
		if iter.txnID > 0 && entry.TxnID > iter.txnID {
			continue // 跳过未来交易的条目
		}
		// 保留每个键的最新可见版本
		if existing, exists := keyMap[entry.Key]; !exists || entry.TxnID > existing.TxnID {
			keyMap[entry.Key] = &entry
		}
	}

	//将映射转换为排序切片
	iter.aggregated = make([]iterator.Entry, 0, len(keyMap))
	for _, entry := range keyMap {
		iter.aggregated = append(iter.aggregated, *entry)
	}

	// sort by key
	for i := 0; i < len(iter.aggregated); i++ {
		for j := i + 1; j < len(iter.aggregated); j++ {
			if iter.aggregated[i].Key < iter.aggregated[j].Key {
				iter.aggregated[i], iter.aggregated[j] = iter.aggregated[j], iter.aggregated[i]
			}
		}
	}
}

// Valid  如果迭代器指向有效条目，则返回 true
func (iter *BlockIterator) Valid() bool {
	return !iter.closed && iter.index >= 0 && iter.index < len(iter.aggregated)
}

// Key 返回当前条目的键
func (iter *BlockIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Key
}

// Value 返回当前条目的值
func (iter *BlockIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Value
}

// TxnID 返回当前条目的事务 ID
func (iter *BlockIterator) TxnID() uint64 {
	if !iter.Valid() {
		return 0
	}
	return iter.aggregated[iter.index].TxnID
}

// IsDeleted 如果当前条目是删除标记，则返回 true
func (iter *BlockIterator) IsDeleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.aggregated[iter.index].Value == ""
}

// Entry e
func (iter *BlockIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.aggregated[iter.index]
}

// Next xiayig
func (iter *BlockIterator) Next() {
	if !iter.Valid() {
		return
	}
	//
	iter.index++
}

// seekToKey 将迭代器定位到聚合条目中第一个具有键 >= 目标的条目
func (iter *BlockIterator) seekToKey(key string) int {
	//聚合条目中的二进制搜索
	left, right := 0, len(iter.aggregated)
	for left < right {
		mid := left + (right-left)/2
		if iter.aggregated[mid].Key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// Seek 将迭代器定位到键 >= 目标的第一个条目处
func (iter *BlockIterator) Seek(key string) bool {
	if iter.closed {
		return false
	}
	//
	iter.index = iter.seekToKey(key)

	//
	if iter.Valid() && iter.aggregated[iter.index].Key == key {
		return true
	}
	return false
}

// SeekToFirst positions the iterator at the first entry
func (iter *BlockIterator) SeekToFirst() {
	if iter.closed {
		return
	}
	iter.index = 0
}

// SeekToLast positions the iterator at the last entry
func (iter *BlockIterator) SeekToLast() {
	if iter.closed {
		return
	}

	iter.index = len(iter.aggregated) - 1
}

// GetType returns the iterator type
func (iter *BlockIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases any resources held by the iterator
func (iter *BlockIterator) Close() {
	iter.closed = true
	iter.block = nil
}

// SetTxnID 设置用于可见性检查的交易 ID
func (iter *BlockIterator) SetTxnID(txnID uint64) {
	if iter.txnID != txnID {
		iter.txnID = txnID
		// Re
		iter.buildAggregatedEntries()
		//
		iter.index = -1
	}
}
