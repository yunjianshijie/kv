package skiplist

import (
	"kv/pkg/iterator"
)

// Node represents a node in the skip list
type Node struct {
	entry    iterator.Entry // 带有事务的键值对
	forward  []*Node        // forward pointers for different levels 不同级别的前向指针 当前节点的上下层的下一个节点）
	backward *Node          // backward pointer to previous node     指向前一个结点 （指向的都是第0层的）
	level    int            // number of levels for this node        这个结点在多少层
}

// 创建新的跳过列表节点
func NewNode(key string, value string, txnID uint64, level int) *Node {
	return &Node{
		entry: iterator.Entry{
			Key:   key,
			Value: value,
			TxnID: txnID,
		},
		forward:  make([]*Node, level),
		backward: nil,
		level:    level,
	}
}

// Key returns the key of the node
func (n *Node) Key() string {
	return n.entry.Key
}

// Value returns the value of the node
func (n *Node) Value() string {
	return n.entry.Value
}

// TxnID returns the transaction ID of the node
func (n *Node) TxnID() uint64 {
	return n.entry.TxnID
}

// IsDeleted returns whether this node represents a delete markert 返回此节点是否代表删除标记 -->通过是否在是entry.value是否有无
func (n *Node) IsDeleted() bool {
	return n.entry.Value == ""
}

// Entry 返回enrty
func (n *Node) Entry() iterator.Entry {
	return n.entry
}

// Level returns the level of the node
func (n *Node) Level() int {
	return n.level
}

// Next returns the next node at level 0
func (n *Node) Next() *Node {
	if len(n.forward) > 0 {
		return n.forward[0]
	}
	return nil
}

// ForwardAt returns the forward pointer at the specified level 返回指定级别的前向指针
func (n *Node) ForwardAt(level int) *Node {
	if level >= 0 && level < len(n.forward) {
		return n.forward[level]
	}
	return nil
}

// SetForwardAt sets the forward pointer at the specified level 将前向指针设置在指定的级别
func (n *Node) SetForwardAt(level int, node *Node) {
	if level >= 0 && level < len(n.forward) {
		n.forward[level] = node
	}
}

// CompareNode compares this node with another node
// Returns:
//
//	-1 if this node < other
//	 0 if this node == other
//	 1 if this node > other
//
// CompareNode 将此节点与另一个节点进行比较
// 返回：
//
// 如果此节点 < 其他节点，则返回 -1
// 如果此节点 == 其他节点，则返回 0
// 如果此节点 > 其他节点，则返回 1
func (n *Node) CompareNode(other *Node) int {
	return iterator.CompareEntries(n.entry, other.entry)
}

// CompareKey compares this node's key with the given key
func (n *Node) CompareKey(key string) int {
	return iterator.CompareKeys(n.entry.Key, key)
}
