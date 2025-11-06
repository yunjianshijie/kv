package sst

import (
	"encoding/binary"
	"kv/pkg/block"
	"kv/pkg/cache"
	"kv/pkg/iterator"
	"kv/pkg/utils"
	"os"
)

// SST 表示一个有序字符串表文件
// 文件结构：
// | 区块部分 | 元部分 | 布隆过滤器 | 元偏移量(4) | 布隆过滤器偏移量(4) | 最小交易ID(8) | 最大交易ID(8) |
type SST struct {
	sstID       uint64             // SST file ID 文件id
	file        *os.File           // File handle 文件handle
	filePath    string             // File path	  文件路径
	metaEntries []BlockMeta        // Block metadata
	metaOffset  uint32             // Offset of metadata section
	bloomOffset uint32             // Offset of bloom filter
	firstKey    string             // First key in SST
	lastKey     string             // Last key in SST
	bloomFilter *utils.BloomFilter // Bloom filter for keys
	blockCache  *cache.BlockCache  // Block cache
	minTxnID    uint64             // Minimum transaction ID
	maxTxnID    uint64             // Maximum transaction ID
	fileSize    int64              // Total file size
}

// OpenSST 使用文件管理器打开 SST 文件（测试兼容性）
func OpenSST(sstID uint64, filePath string, blockCache *cache.BlockCache, fileManager *utils.FileManager) (*SST, error) {
	return Open(sstID, filePath, blockCache)
}

// Open 打开现有的 SST 文件
func Open(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	//
	FileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	// 文件大小
	fileSize := FileInfo.Size()
	if fileSize < 24 { // // Min size: 4+4+8+8 = 24 bytes for offsets and transaction IDs
		file.Close()
		return nil, utils.ErrInvalidSSTFile
	}

	//
	sst := &SST{
		sstID:      sstID,
		file:       file,
		filePath:   filePath,
		blockCache: blockCache,
		fileSize:   fileSize,
	}

	//读取页脚：MetaOffset(4) + BloomOffset(4) + MinTxnID(8) + MaxTxnID(8)
	footerSize := int64(24)
	footer := make([]byte, footerSize)
	_, err = file.ReadAt(footer, fileSize-footerSize)
	if err != nil {
		file.Close()
		return nil, err
	}

	// parse footer
	sst.metaOffset = binary.LittleEndian.Uint32(footer[0:4])
	sst.bloomOffset = binary.LittleEndian.Uint32(footer[4:8])
	sst.minTxnID = binary.LittleEndian.Uint64(footer[8:16])
	sst.maxTxnID = binary.LittleEndian.Uint64(footer[16:24])

	// 验证偏移量
	if int64(sst.metaOffset) >= fileSize || int64(sst.metaOffset) >= fileSize {
		file.Close()
		return nil, utils.ErrCorruptedFile
	}

	// 读取并解码元数据
	metaSize := sst.bloomOffset - sst.metaOffset
	MetaData := make([]byte, metaSize)
	_, err = file.ReadAt(MetaData, int64(sst.metaOffset))
	if err != nil {
		file.Close()
		return nil, err
	}

	// 设置第一个和最后一个关键帧
	if len(sst.metaEntries) > 0 {
		sst.firstKey = sst.metaEntries[0].FirstKey
		sst.lastKey = sst.metaEntries[len(sst.metaEntries)-1].LastKey
	}

	// 存果存在，则读取布隆过滤器
	bloomSize := int64(sst.metaOffset) - int64(sst.bloomOffset)
	if bloomSize > 0 {
		bloomData := make([]byte, bloomSize)
		_, err = file.ReadAt(bloomData, int64(sst.bloomOffset))
		if err != nil {
			file.Close()
			return nil, err
		}
		sst.bloomFilter = utils.DeserializeBloomFilter(bloomData)
	}
	//
	return sst, nil
}

// Close closes the SST file
func (sst *SST) Close() error {
	if sst.file != nil {
		return sst.file.Close()
	}
	return nil
}

// MetaOffset 返回meta偏移量
func (sst *SST) MetaOffset() int64 {
	return int64(sst.metaOffset)
}

// ID returns the SST ID
func (sst *SST) ID() uint64 {
	return sst.sstID
}

// FirstKey returns the first key in the SST
func (sst *SST) FirstKey() string {
	return sst.firstKey
}

// LastKey returns the last key in the SST
func (sst *SST) LastKey() string {
	return sst.lastKey
}

// Size returns the file size
func (sst *SST) Size() int64 {
	return sst.fileSize
}

// NumBlocks returns the number of blocks in the SST
func (sst *SST) NumBlocks() int {
	return len(sst.metaEntries)
}

// GetSSTID returns the SST ID (alias for ID)
func (sst *SST) GetSSTID() uint64 {
	return sst.sstID
}

// GetFirstKey returns the first key in the SST (alias for FirstKey)
func (sst *SST) GetFirstKey() string {
	return sst.firstKey
}

// GetLastKey returns the last key in the SST (alias for LastKey)
func (sst *SST) GetLastKey() string {
	return sst.lastKey
}

// GetNumBlocks returns the number of blocks in the SST (alias for NumBlocks)
func (sst *SST) GetNumBlocks() int {
	return len(sst.metaEntries)
}

// TxnRange returns the transaction ID range
func (sst *SST) TxnRange() (uint64, uint64) {
	return sst.minTxnID, sst.maxTxnID
}

// ReadBlock reads a block by index
func (sst *SST) ReadBlock(blockIdx int) (*block.Block, error) {
	if blockIdx < 0 || blockIdx >= len(sst.metaEntries) {
		return nil, utils.ErrBlockNotFound
	}

	// 先试试缓存有没有
	if sst.blockCache != nil {
		if cached := sst.blockCache.Get(sst.sstID, uint64(blockIdx)); cached != nil {
			return cached, nil
		}
	}
	// 计算块大小
	meta := sst.metaEntries[blockIdx]
	var blockSize uint32
	if blockIdx == len(sst.metaEntries)-1 {
		// Last block:
		blockSize = sst.metaOffset - meta.Offset
	} else {
		//
		blockSize = sst.metaEntries[blockIdx+1].Offset - meta.Offset
	}

	// 读取块数据
	blockData := make([]byte, blockSize)
	_, err := sst.file.ReadAt(blockData, int64(meta.Offset))
	if err != nil {
		return nil, err
	}

	// 解码块
	blk, err := block.NewBlock(blockData)
	if err != nil {
		return nil, err
	}

	// Cache the block
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.sstID, uint64(blockIdx), blk)
	}

	return blk, nil
}

// FindBlockIndex 查找可能包含键的块索引
func (sst *SST) FindBlockIndex(key string) int {
	// 首先检查布隆过滤器（仅针对应该在SST中的键）
	if sst.bloomFilter != nil && key >= sst.firstKey && key <= sst.lastKey && !sst.bloomFilter.Contains(key) {
		return -1 // Key definitely not in this SST
	}

	//如果键位于最后一个键之后，则它不在任何块中。
	if key > sst.lastKey {
		return -1
	}

	// 如果某个键位于第一个键之前，则应将其放入第一个块。
	if key < sst.firstKey {
		return 0
	}

	// 对块元数据进行二分查找
	left, right := 0, len(sst.metaEntries)
	for left < right {
		mid := (left + right) / 2
		meta := sst.metaEntries[mid]

		if key < meta.LastKey {
			right = mid
		} else if key > meta.LastKey {
			left = mid + 1
		} else {
			return mid /// 找到了包含此键范围的块
		}
	}

	// Binary
	if left < len(sst.metaEntries) {
		return left
	}
	return -1
}

// FindBlockIdx is an alias for FindBlockIndex (test compatibility)
func (sst *SST) FindBlockIdx(key string) int {
	return sst.FindBlockIndex(key)
}

// Get searches for a key in the SST
func (sst *SST) Get(key string, txnID uint64) (iterator.Iterator, error) {
	blockIdx := sst.FindBlockIdx(key)
	if blockIdx == -1 {
		return iterator.NewEmptyIterator(), nil
	}
	blk, err := sst.ReadBlock(blockIdx)

	if err != nil {
		return nil, err
	}

	blockIter := blk.NewIterator()
	blockIter.Seek(key)

	// 返回
	return &SSTIterator{
		sst:       sst,
		blockIdx:  blockIdx,
		blockIter: blockIter,
		txnID:     txnID,
	}, nil
}

// NewIterator creates a new iterator for the SST
func (sst *SST) NewIterator(txnID uint64) iterator.Iterator {
	return &SSTIterator{
		sst:       sst,
		blockIdx:  0,
		blockIter: nil, // Will be initialized on first access
		txnID:     txnID,
	}
}

// Delete deletes the SST file
func (sst *SST) Delete() error {
	if sst.file != nil {
		sst.file.Close()
		sst.file = nil
	}
	return os.Remove(sst.filePath)
}
