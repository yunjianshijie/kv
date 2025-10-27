package cache

import (
	"container/heap"
	"kv/pkg/block"
	"sync"
	"time"
)

// BlockCacheKey 表示块的缓存键 SSTable 数据块缓存块的唯一标识
type BlockCacheKey struct {
	SSTID   uint64 // SST file ID
	BlockID uint64 // Block index within the SST
}

// cacheEntry 表示缓存项 缓存中的具体条目(值)
type cacheEntry struct {
	key        BlockCacheKey
	block      *block.Block
	timestamps []int64 // 记录最近访问的时间戳列表

	// 对于堆实现
	index int // 堆中项目的索引
}

// BlockCache 实现块的 LFU 缓存  Least Frequently Used（最不经常使用）
type BlockCache struct {
	capacity int
	maxFreq  int // 时间戳列表的最大长度
	cache    map[BlockCacheKey]*cacheEntry
	mutex    sync.RWMutex

	// For frequency based eviction  对于基于频率的驱逐
	freqHeap *entryHeap
}

// entryHeap 实现 heap.Interface 并保存缓存条目
type entryHeap []*cacheEntry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	// 我们希望 Pop 能返回频率最低的条目
	// 如果频率相等，则优先选择最近最少使用的条目
	if len(h[i].timestamps) == len(h[j].timestamps) {
		// 如果都为空，按索引排序
		if len(h[i].timestamps) == 0 {
			return h[i].index < h[j].index
		}
		// 优先驱逐最近访问时间更久远的
		return h[i].timestamps[len(h[i].timestamps)-1] < h[j].timestamps[len(h[j].timestamps)-1]
	}
	return len(h[i].timestamps) < len(h[j].timestamps)
}

func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = j
	h[j].index = i
}

func (h *entryHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*cacheEntry)
	entry.index = n
	*h = append(*h, entry)
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[0 : n-1]
	return entry
}

// NewBlockCache 创建具有指定容量的新块缓存
func NewBlockCache(capacity int) *BlockCache {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	bc := &BlockCache{
		capacity: capacity,
		maxFreq:  32, // 默认最大频率限制
		cache:    make(map[BlockCacheKey]*cacheEntry),
		freqHeap: &entryHeap{},
	}
	heap.Init(bc.freqHeap)
	return bc
}

// Get 从缓存中检索一个块
func (bc *BlockCache) Get(sstID, blockID uint64) *block.Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	//
	if entry, found := bc.cache[key]; found {
		//
		entry.timestamps = append(entry.timestamps, time.Now().UnixNano())

		//
		if len(entry.timestamps) > bc.maxFreq {
			entry.timestamps = entry.timestamps[1:]
		}
		heap.Fix(bc.freqHeap, entry.index) // 访问最小堆的结构
		return entry.block
	}
	return nil
}

// Put adds a block to the cache
func (bc *BlockCache) Put(sstID, blockID uint64, block *block.Block) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}

	//如果已经存在，则更新并记录时间戳
	if entry, found := bc.cache[key]; found {
		entry.block = block
		// 记录时间挫
		entry.timestamps = append(entry.timestamps, time.Now().UnixNano())

		// 如果最大长度
		if len(entry.timestamps) > bc.maxFreq {
			entry.timestamps = entry.timestamps[1:]
		}
		heap.Fix(bc.freqHeap, entry.index)
		return
	}

	// 如果容量超过，则驱逐最不常用的用户
	if len(bc.cache) >= bc.capacity {
		bc.evict()
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		block:      block,
		timestamps: []int64{time.Now().UnixNano()},
	}
	bc.cache[key] = entry

	// add to heap
	heap.Push(bc.freqHeap, entry)
}

// Remove 从缓存中删除特定块
func (bc *BlockCache) Remove(sstID, blockID uint64) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if entry, found := bc.cache[key]; found {
		// Remove from heap
		heap.Remove(bc.freqHeap, entry.index)
		delete(bc.cache, key)
	}
}

// Clear removes all entries from the cache
func (bc *BlockCache) Clear() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.cache = make(map[BlockCacheKey]*cacheEntry)
	bc.freqHeap = &entryHeap{}
	heap.Init(bc.freqHeap)
}

// Size returns the current number of cached blocks
func (bc *BlockCache) Size() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return len(bc.cache)
}

// Capacity 返回缓存的最大容量
func (bc *BlockCache) Capacity() int {
	return bc.capacity
}

// Stats returns cache statistics
func (bc *BlockCache) Stats() BlockCacheStats {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	return BlockCacheStats{
		Size:     len(bc.cache),
		Capacity: bc.capacity,
	}
}

// evict 删除最不常用的条目
func (bc *BlockCache) evict() {
	if bc.freqHeap.Len() == 0 {
		return
	}

	//获取频率最低的条目
	entry := heap.Pop(bc.freqHeap).(*cacheEntry)

	// Remove from map
	delete(bc.cache, entry.key)
}

// BlockCacheStats 表示缓存统计信息
type BlockCacheStats struct {
	Size     int
	Capacity int
}
