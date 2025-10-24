package iterator

import (
	"errors"
)

type IteratorType int

const (
	// SkipListIteratorType for skip list iterators
	SkipListIteratorType IteratorType = iota
	// SSTIteratorType for SST iterators
	SSTIteratorType
	// HeapIteratorType for heap-based merge iterators
	HeapIteratorType
	// MergeIteratorType for general merge iterators
	MergeIteratorType
	// SelectIteratorType for select iterators
	SelectIteratorType
	// ConcatIteratorType for concat iterators
	ConcatIteratorType
)

// Entry represents a key-value entry with transaction ID表示带有交易 ID 的键值条目
type Entry struct {
	Key   string
	Value string
	TxnID uint64
}

// Iterator 是系统中所有迭代器的基础接口
type Iterator interface {
	// Valid returns true if the iterator is pointing to a valid entry
	Valid() bool

	// Key returns the key of the current entry
	// Only valid when Valid() returns true
	Key() string

	// Value returns the value of the current entry
	// Only valid when Valid() returns true
	Value() string

	// TxnID returns the transaction ID of the current entry
	TxnID() uint64

	// IsDeleted returns true if the current entry is a delete marker
	IsDeleted() bool

	// Entry returns the current entry
	Entry() Entry

	// Next advances the iterator to the next entry
	Next()

	// Seek positions the iterator at the first entry with key >= target
	Seek(key string) bool

	// SeekToFirst positions the iterator at the first entry
	SeekToFirst()

	// SeekToLast positions the iterator at the last entry
	SeekToLast()

	// GetType returns the iterator type
	GetType() IteratorType

	// Close releases any resources held by the iterator
	Close()
}

// common errors
var (
	ErrInvalidIterator = errors.New("invalid iterator")
	ErrIteratorClosed  = errors.New("iterator is closed")
)

// EmptyIterator 是一个不包含任何条目的迭代器
type EmptyIterator struct {
	closed bool
}

// NewEmptyIterator 创建新的空迭代器
func NewEmptyIterator() *EmptyIterator {
	return &EmptyIterator{closed: false}
}

func (e *EmptyIterator) Valid() bool           { return false }
func (e *EmptyIterator) Key() string           { return "" }
func (e *EmptyIterator) Value() string         { return "" }
func (e *EmptyIterator) TxnID() uint64         { return 0 }
func (e *EmptyIterator) IsDeleted() bool       { return false }
func (e *EmptyIterator) Next()                 {}
func (e *EmptyIterator) Seek(key string) bool  { return false }
func (e *EmptyIterator) SeekToFirst()          {}
func (e *EmptyIterator) SeekToLast()           {}
func (e *EmptyIterator) GetType() IteratorType { return SkipListIteratorType }
func (e *EmptyIterator) Close()                { e.closed = true }

func (e *EmptyIterator) Entry() Entry {
	return Entry{}
}

// CompareKeys 按字典顺序比较两个键
func CompareKeys(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// CompareEntries 首先按键比较两个条目，然后按交易 ID 比较（先比较较高的交易 ID）
func CompareEntries(a, b Entry) int {
	cmp := CompareKeys(a.Key, b.Key)
	if cmp != 0 {
		return cmp
	}

	if a.TxnID == 0 || b.TxnID == 0 {
		return cmp
	}
	// Same key, higher transaction ID has priority (comes first)相同密钥，交易 ID 越高，优先级越高（先到）
	if a.TxnID > b.TxnID {
		return -1
	}
	if a.TxnID < b.TxnID {
		return 1
	}
	return 0
}
