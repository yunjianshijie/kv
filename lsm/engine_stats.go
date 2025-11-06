package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
)

// EngineStatistics 包含 LSM 发动机的统计数据
type EngineStatistics struct {
	// Read/Write statistics
	Reads   uint64
	Writes  uint64
	Deletes uint64

	// Flush statistics
	Flushes      uint64
	FlushedBytes uint64

	// Compaction statistics
	Compactions    uint64
	CompactedBytes uint64
	CompactedFiles uint64

	// Memory statistics
	MemTableSize    uint64
	FrozenTableSize uint64

	// SST statistics
	SSTFiles     uint64
	TotalSSTSize uint64
}

// EngineMetadata 代表需要持久化的元数据
type EngineMetadata struct {
	NextSSTID       uint64 `json:"next_sst_id"`
	NextTxnID       uint64 `json:"next_txn_id"`
	GlobalReadTxnID uint64 `json:"global_read_txn_id"`
}

// saveMetadata 将引擎元数据保存到磁盘
func saveMetadata(e *Engine)error{
	//metadata用json格式写入metadataFile
	data, err := json.MarshalIndent(e.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err))
	}
	if err := os.WriteFile(e.metadataFile,data,0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}
	return nil
}


// loadMetadata 从磁盘加载引擎元数据
func loadMetadata(e *Engine)error{
	//
	if _, err := os.Stat(e.metadataFile); os.IsNotExist(err) {
		// 没有现有的元数据文件，使用默认值。  原子写入操作赋值 011
		atomic.StoreUint64(&e.metadata.NextSSTID, 0)
		atomic.StoreUint64(&e.metadata.NextTxnID, 1)
		atomic.StoreUint64(&e.metadata.GlobalReadTxnID, 1)
	}

	// read file
	data,err := os.ReadFile(e.metadataFile)
	if err != nil {return fmt.Errorf("failed to read metadata file: %w", err)}

	//
	if e.metadata == nil{
		e.metadata = &EngineMetadata{}
	}
	if err := json.Unmarshal(data,&e.metadata);err != nil {
		return fmt.Errorf("failed to unmarshal metadata file: %w", err)
	}
	return nil
}


