package block

import (
	"encoding/binary"
	"fmt"
	"kv/pkg/iterator"
	"kv/pkg/logger"
)

// Block 表示 SST 文件中的数据块
type Block struct {
	data      []byte           // 原始块数据
	offsets   []uint16         // 每个键值对的偏移量
	entries   []iterator.Entry // 缓存条目以便更快地访问
	blockSize int              // 目标块大小
}

// BlockBuilder 帮助逐步构建块
type BlockBuilder struct {
	data      []byte   // 存储块的 “实际数据内容”，是一个字节切片，用于累积所有条目（Entry）序列化后的字节流
	offsets   []uint16 // 条目偏移量列表
	blockSize int      // 最大大小限制 （字节）
	firstKey  string   // 第一个条目的键key
	lastKey   string   // 最后一个条目的键key
}

// NewBlockBuilder 创建具有指定目标大小的新块构建器
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		data:      make([]byte, 0),
		offsets:   make([]uint16, 0),
		blockSize: blockSize,
	}
}

// Add 向正在构建的块中添加一个键值条目
func (bb *BlockBuilder) Add(key, value string, txnID uint64, forceFlush bool) error {
	//计算此条目所需的大小
	entrySize := 8 + 2 + len(key) + 2 + len(value) //  txnID  + keyLen + key + valueLen + value

	// 检查添加此条目是否会超出块大小  len(bb.data)+entrySize+len(bb.offsets)*2+2 是条目大小+
	if !forceFlush && len(bb.data)+entrySize+len(bb.offsets)*2+2 > bb.blockSize && len(bb.offsets) > 0 {
		return fmt.Errorf("block size limit exceeded")
	}

	// 记录该条目的偏移量
	bb.offsets = append(bb.offsets, uint16(len(bb.data)))

	// 对条目进行编码
	// 格式：[txnID:8][keyLen:2][key][valueLen:2][value]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, txnID) // 按小端字节序
	bb.data = append(bb.data, buf...)

	// 密钥长度和密钥
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(key)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(key)...)

	// Value length and value
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(value)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(value)...)

	// Update first and last keys
	if bb.firstKey == "" {
		bb.firstKey = key
	}
	bb.lastKey = key

	return nil
}

// EstimatedSize 返回当前区块的预估大小
func (bb *BlockBuilder) EstimatedSize() int {
	return len(bb.data) + bb.blockSize // blockSize
}

// DataSize data大小
func (bb *BlockBuilder) DataSize() int {
	return len(bb.data)
}

// IsEmpty 如果块构建器为空，IsEmpty 返回 true
func (bb *BlockBuilder) IsEmpty() bool {
	return len(bb.offsets) == 0
}

// FirstKey 返回块中的第一个键
func (bb *BlockBuilder) FirstKey() string {
	return bb.firstKey
}

// LastKey 返回块中的最后一个键
func (bb *BlockBuilder) LastKey() string {
	return bb.lastKey
}

// Build 完成并返回构建的块
func (bb *BlockBuilder) Build() *Block {
	if len(bb.offsets) == 0 {
		// 空块：只有条目数（0）
		emptyDate := make([]byte, 2)
		binary.LittleEndian.PutUint16(emptyDate, 0) // 16b
		return &Block{
			data:      emptyDate,
			offsets:   []uint16{},
			entries:   []iterator.Entry{},
			blockSize: bb.blockSize,
		}
	}
	// 在末尾构建带有偏移量的最终数据
	finalDate := make([]byte, 0, len(bb.data)+len(bb.offsets)*2+2)
	finalDate = append(finalDate, bb.data...)

	// Append offsets
	for _, offset := range bb.offsets {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, offset)
		finalDate = append(finalDate, buf...)
	}

	// Append number of entries
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(len(bb.offsets)))
	filalDate := append(finalDate, buf...)

	// 解析缓存条目entries
	entries, _ := parseEntries(finalDate, bb.offsets)

	return &Block{
		data:      filalDate,
		offsets:   bb.offsets,
		entries:   entries,
		blockSize: bb.blockSize,
	}
}

// parseEntries 将原始数据解析为条目
func parseEntries(data []byte, offsets []uint16) ([]iterator.Entry, error) {
	//
	entries := make([]iterator.Entry, len(offsets))

	for i, offset := range offsets {
		// 基础字段都有（每个都有）
		if int(offset)+8+1+2 >= len(data) {
			return nil, fmt.Errorf("block size limit exceeded")
		}
		//
		pos := int(offset)
		// Read transaction ID 改成大端序
		txnID := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		//  Read key length and key
		keyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(keyLen) > len(data) {
			return nil, fmt.Errorf("invalid key length")
		}

		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		// 读取值的长度和值
		if pos+2 > len(data) {
			return nil, fmt.Errorf("invalid value length position")
		}

		valueLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(valueLen) > len(data) {
			return nil, fmt.Errorf("invalid value length")
		}

		value := string(data[pos : pos+int(valueLen)])

		entries[i] = iterator.Entry{
			Key:   key,
			Value: value,
			TxnID: txnID,
		}
	}
	return entries, nil
}

// NewBlock 根据原始数据创建一个块
func NewBlock(data []byte) (*Block, error) {
	// 如果data大小小于2
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid block data: too short")
	}

	//从末尾读取条目数 numEntries 是这个
	numEntries := binary.LittleEndian.Uint16(data[len(data)-2:])

	//
	if len(data) < int(numEntries)*2+2 {
		return nil, fmt.Errorf("invalid block data: insufficient data for offsets")
	}

	// Read offsets
	offsets := make([]uint16, numEntries)
	offsetStart := len(data) - 2 - int(numEntries)*2

	for i := 0; i < int(numEntries); i++ {
		offset := offsetStart + i*2
		offsets[i] = binary.LittleEndian.Uint16(data[offset : offset+2])
	}

	// Parse entries
	actualData := data[:offsetStart]
	entries, err := parseEntries(actualData, offsets)
	if err != nil {
		return nil, err
	}

	return &Block{
		data:    data,
		offsets: offsets,
		entries: entries,
	}, nil

}

// NumEntries 返回块中的条目数
func (b *Block) NumEntries() int {
	return len(b.offsets)
}

// GetEntry 返回指定索引处的条目
func (b *Block) GetEntry(index int) (iterator.Entry, error) {
	if index < 0 || index >= len(b.entries) {
		return iterator.Entry{}, fmt.Errorf("index out of bounds")
	}
	return b.entries[index], nil
}

// Data 返回原始块数据
func (b *Block) Data() []byte {
	return b.data
}

// Size returns the size of the block in bytes
func (b *Block) Size() int {
	return len(b.data)
}

// FirstKey 返回块中的第一个键
func (b *Block) FirstKey() string {
	if len(b.offsets) == 0 {
		return ""
	}
	return b.entries[0].Key
}

// LastKey returns the last key in the block
func (b *Block) LastKey() string {
	if len(b.offsets) == 0 {
		return ""
	}
	return b.entries[len(b.entries)-1].Key
}

// FindEntry 查找第一个键值大于等于目标值的条目的索引
// 如果不存在这样的条目，则返回 -1
func (b *Block) FindEntry(key string) int {
	//
	left, right := 0, len(b.entries)
	// 二分查找
	for left < right {
		mid := (left + right) / 2
		if iterator.CompareKeys(b.entries[mid].Key, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// left 最后的
	if left >= len(b.entries) {
		return -1
	}

	return left
}

// GetNumEntries 返回块中的条目数（NumEntries 的别名）
func (b *Block) GetNumEntries() int {
	return len(b.offsets)
}

// GetValue 搜索键并返回其值（支持 MVCC）
// 返回 (value, found, error)
func (b *Block) GetValue(key string, txnID uint64) (string, bool) {
	// 收集具有目标键的所有条目
	var matchingEntries []iterator.Entry
	for _, entry := range b.entries {
		if entry.Key == key {
			matchingEntries = append(matchingEntries, entry)
		}
	}
	if len(matchingEntries) == 0 {
		return "", false
	}

	// 调试：打印所有匹配的条目
	logger.Tracef("GetValue(%s, %d): found %d entries\n", key, txnID, len(matchingEntries))
	for _, entry := range matchingEntries {
		logger.Tracef(" TxnID=%d, Value=%s\n", txnID, entry.Value)
	}
	// 如果 txnID 为 0，则返回最新版本
	if txnID == 0 {
		//查找具有最高交易 ID 的条目
		latestEntry := matchingEntries[0]
		for _, entry := range matchingEntries {
			if entry.TxnID > txnID {
				latestEntry = entry
			}
		}
		return latestEntry.Value, true
	}

	// 查找 <= txnID 的最新版本
	var bestEntry *iterator.Entry
	for i, entry := range matchingEntries {
		if entry.TxnID <= txnID {
			if bestEntry == nil || entry.TxnID > bestEntry.TxnID {
				bestEntry = &matchingEntries[i]
			}
		}
	}

	//
	if bestEntry != nil {
		logger.Debugf("  Best entry: TxnID=%d, Value=%s\n", bestEntry.TxnID, bestEntry.Value)
	}

	if bestEntry != nil {
		return bestEntry.Value, true
	}

	return "", false
}

// NewIterator 为该块创建一个新的迭代器
func (b *Block) NewIterator() iterator.Iterator {
	return NewBlockIterator(b)
}
