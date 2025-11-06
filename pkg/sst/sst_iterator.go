package sst

import (
	"encoding/binary"
	"kv/pkg/block"
	"kv/pkg/cache"
	"kv/pkg/config"
	"kv/pkg/iterator"
	"kv/pkg/utils"
	"os"
)

// SSTBuilder builds SST files
type SSTBuilder struct {
	blockBuilder *block.BlockBuilder
	metas        []BlockMeta
	data         []byte
	blockSize    int
	bloomFilter  *utils.BloomFilter
	firstKey     string
	lastKey      string
	minTxnID     uint64
	maxTxnID     uint64
	hasBloom     bool
}



// NewSSTBuilder creates a new SST builder
func NewSSTBuilder(blockSize int, hasBloom bool) *SSTBuilder {
	var bloomFilter *utils.BloomFilter
	if hasBloom {
		cfg := config.GetGlobalConfig()
		bloomFilter = utils.NewBloomFilter(
			cfg.BloomFilter.ExpectedSize,
			cfg.BloomFilter.ExpectedErrorRate,
		)
	}
	return &SSTBuilder{
		blockBuilder: block.NewBlockBuilder(blockSize),
		metas:        make([]BlockMeta, 0),
		data:         make([]byte, 0),
		blockSize:    blockSize,
		bloomFilter:  bloomFilter,
		minTxnID:     ^uint64(0), // Max uint64
		maxTxnID:     0,
		hasBloom:     hasBloom,
	}
}

func (builder *SSTBuilder) GetDataSize() int {
	return builder.blockBuilder.DataSize() + len(builder.data)
}

// Add adds a key-value pair to the SST
func (builder *SSTBuilder) Add(key, value string, txnID uint64) error {
	//
	if builder.firstKey == "" {
		builder.firstKey = key
	}
	//
	if txnID < builder.minTxnID {
		builder.minTxnID = txnID
	}
	if txnID > builder.maxTxnID {
		builder.maxTxnID = txnID
	}

	if builder.bloomFilter != nil {
		builder.bloomFilter.Add(key)
	}

	forceFlush := key == builder.lastKey

	//
	err := builder.blockBuilder.Add(key, value, txnID, forceFlush)
	if err != nil {
		builder.lastKey = key
		return nil // 成功添加
	}
	err = builder.finishBlock()
	if err != nil {
		return err
	}

	//
	builder.firstKey = key
	builder.lastKey = key

	return builder.blockBuilder.Add(key, value, txnID, false)

}

// finishBlock completes the current block and starts a new one完成当前代码块并开始新代码块。
func (builder *SSTBuilder) finishBlock() error {
	if builder.blockBuilder.IsEmpty() {
		return nil
	}
	//
	blk := builder.blockBuilder.Build()
	blockData := blk.Data()

	//
	meta := NewBlockMeta(uint32((len(builder.data)),builder.blockBuilder.FirstKey())
	builder.metas = append(builder.metas, meta)

	builder.data = append(builder.data, meta)

	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)

	return nil
}

// Build builds the SST file
func (builder *SSTBuilder) Build(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	// Finish the last block
	if !builder.blockBuilder.IsEmpty() {
		if err := builder.finishBlock(); err != nil {
			return nil, err
		}
	}

	if len(builder.metas) == 0 {
		return nil, utils.ErrEmptySST
	}

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Write blocks data
	_, err = file.Write(builder.data)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Record metadata offset
	metaOffset := uint32(len(builder.data))

	// Write metadata
	metaData := EncodeBlockMetas(builder.metas)
	_, err = file.Write(metaData)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Record bloom filter offset
	bloomOffset := uint32(len(builder.data) + len(metaData))

	// Write bloom filter
	if builder.bloomFilter != nil {
		bloomData := builder.bloomFilter.Serialize()
		_, err = file.Write(bloomData)
		if err != nil {
			file.Close()
			os.Remove(filePath)
			return nil, err
		}
	}

	// Write footer
	footer := make([]byte, 24)
	binary.LittleEndian.PutUint32(footer[0:4], metaOffset)
	binary.LittleEndian.PutUint32(footer[4:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], builder.minTxnID)
	binary.LittleEndian.PutUint64(footer[16:24], builder.maxTxnID)

	_, err = file.Write(footer)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Sync file
	err = file.Sync()
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	file.Close()

	// Open the built SST
	return Open(sstID, filePath, blockCache)
}


// SSTIterator implements iterator for SST
type SSTIterator struct {
	sst       *SST
	blockIdx  int
	blockIter iterator.Iterator
	txnID     uint64
}


// Valid returns true if the iterator is pointing to a valid entry
func (iter *SSTIterator) Valid() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return true
	}
	return false
}

// Key returns the key of the current entry
func (iter *SSTIterator) Key() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Key()
	}
	return ""
}

// Value returns the value of the current entry
func (iter *SSTIterator) Value() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Value()
	}
	return ""
}

// TxnID returns the transaction ID of the current entry
func (iter *SSTIterator) TxnID() uint64 {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.TxnID()
	}
	return 0
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *SSTIterator) IsDeleted() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.IsDeleted()
	}
	return false
}

// Entry returns the current entry
func (iter *SSTIterator) Entry() iterator.Entry {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Entry()
	}
	return iterator.Entry{}
}

// Next advances the iterator to the next entry
func (iter *SSTIterator) Next() {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		iter.blockIter.Next()
		if !iter.blockIter.Valid() {
			// Move to next block
			iter.moveToNextBlock()
		}
	}
}

// moveToNextBlock moves to the next block
func (iter *SSTIterator) moveToNextBlock() {
	iter.blockIdx++
	if iter.blockIdx >= iter.sst.NumBlocks() {
		// No more blocks
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		return
	}

	// Load next block
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// Seek positions the iterator at the first entry with key >= target
func (iter *SSTIterator) Seek(key string) bool {
	blockIdx := iter.sst.FindBlockIndex(key)
	if blockIdx == -1 {
		// Key not found in any block
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		iter.blockIdx = iter.sst.NumBlocks()
		return false
	}

	iter.blockIdx = blockIdx
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return false
	}

	iter.blockIter = blk.NewIterator()
	found := iter.blockIter.Seek(key)

	// If not found in this block, move to next block
	if !iter.blockIter.Valid() {
		iter.moveToNextBlock()
	}

	return found
}

// SeekToFirst positions the iterator at the first entry
func (iter *SSTIterator) SeekToFirst() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = 0
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(0)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// SeekToLast positions the iterator at the last entry
func (iter *SSTIterator) SeekToLast() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = iter.sst.NumBlocks() - 1
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToLast()
}

// GetType returns the iterator type
func (iter *SSTIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases resources held by the iterator
func (iter *SSTIterator) Close() {
	if iter.blockIter != nil {
		iter.blockIter.Close()
		iter.blockIter = nil
	}
}
