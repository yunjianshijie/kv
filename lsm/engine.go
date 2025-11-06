package lsm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"kv/pkg/cache"
	"kv/pkg/common"
	"kv/pkg/config"
	"kv/pkg/iterator"
	"kv/pkg/logger"
	"kv/pkg/memtable"
	"kv/pkg/sst"
	"kv/pkg/utils"
	"kv/pkg/wal"
)

// Engine 代表 LSM 树存储引擎
type Engine struct {
	//核心部件
	config      *config.Config
	dataDir     string
	memTable    *memtable.MemTable
	blockCache  *cache.BlockCache
	fileManager *utils.FileManager

	// SST management
	levels   *LevelManager
	metadata *EngineMetadata
	// WAL management
	wal *wal.WAL

	// Metadata persistence
	metadataFile string

	// Background workers
	checkCh chan struct{}

	wg sync.WaitGroup

	// Statistics
	stats *EngineStatistics

	txnManager *TransactionManager

	// State
	closed                bool
	flushAndCompactByHand bool // during test, disable background flush and compact to make it easy to debug

	// 控制后台 goroutine 的上下文
	ctx    context.Context
	cancel context.CancelFunc
}

// init
func (e *Engine) initTxnManager(config *TransactionConfig) error {
	if config == nil {
		// 返回 默认配置
		config = DefaultTransactionConfig()
	}
	// 创建一个新的事务管理器
	manger := NewTransactionManager(e, config)
	e.txnManager = manger
	return nil
}

// NewEngine creates a new LSM engine
func NewEngine(cfg *config.Config, dataDir string) (*Engine, error) {
	// 配置是否正确
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	// 如果数据目录不存在，则创建数据目录。()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize file manager 文件管理
	fileManager := utils.NewFileManager(dataDir)

	// Initialize block cache 块管理cache
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Initialize memtable 初始化内存表
	mt := memtable.New()

	// Initialize level manager 初始化级别管理器
	levels := NewLevelManager(cfg, fileManager, blockCache)

	// Initialize WAL
	walDir := filepath.Join(dataDir, "wal")
	walConfig := &wal.Config{
		LogDir:        walDir,
		BufferSize:    int(cfg.GetWALBufferSize() / 64), // Convert bytes to record count estimate
		FileSizeLimit: cfg.GetWALFileSizeLimit(),
		CleanInterval: time.Duration(cfg.GetWALCleanInterval()) * time.Second,
	}

	walInstance, err := wal.New(walConfig, 0) // 从检查点 0 开始
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	engine := &Engine{
		config:      cfg,         // 引擎配置（如层级比例、SST大小等）
		dataDir:     dataDir,     // 数据根目录（所有文件的基础路径）
		memTable:    mt,          // 内存表（MemTable，接收实时写入的有序数据结构）
		blockCache:  blockCache,  // 数据块缓存（加速SST文件的读操作）
		fileManager: fileManager, // 文件管理器（统一处理文件IO）
		levels:      levels,      // LSM树的层级集合（初始可能为空，后续恢复或创建）
		wal:         walInstance, // WAL日志实例（预写日志，保证数据持久化）
		metadata: &EngineMetadata{ // 引擎元数据（核心ID生成器的初始值）
			NextSSTID:       0, // 下一个SST文件ID（从0开始自增）
			NextTxnID:       1, // 下一个事务ID（从1开始，0通常为特殊标识）
			GlobalReadTxnID: 1, // 全局读事务版本号（初始可见版本）
		},
		stats:        &EngineStatistics{},                // 引擎统计信息（如读写次数、合并次数等）
		metadataFile: filepath.Join(dataDir, "metadata"), // 元数据文件路径（持久化元数据）
		closed:       false,                              // 引擎是否已关闭（初始为未关闭）
		checkCh:      make(chan struct{}, 1),             // 用于触发检查的通道（如检查是否需要合并）
	}
	engine.initTxnManager(nil)

	// Create context for background workers
	engine.ctx, engine.cancel = context.WithCancel(context.Background())

	// Load metadata if exists
	if err := loadMetadata(engine); err != nil {
		logger.Warnf("failed to load metadata: %v", err)
	}

	// Recover from existing data if any
	if err := engine.recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	// Save initial metadata
	if err := saveMetadata(engine); err != nil {
		return nil, fmt.Errorf("failed to save initial metadata: %w", err)
	}

	// Start background workers
	engine.startBackgroundWorkers()

	return engine, nil
}

// recover 从磁盘恢复引擎状态
func (e *Engine) recover() error {
	// First, recover SST files
	if err := e.levels.LoadExistingSSTs(); err != nil {
		return fmt.Errorf("failed to load existing SST files: %w", err)
	}

	// Then, recover from WAL
	if e.wal != nil {
		if err := e.recoverFromWAL(); err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	return nil
}

// recoverFromWAL recovers uncommitted transactions from WAL logs
func (e *Engine) recoverFromWAL() error {
	// Read WAL records
	walDir := filepath.Join(e.dataDir, "wal")
	recordsByTxn, err := wal.Recover(walDir, 0) // Recover from checkpoint 0
	if err != nil {
		return fmt.Errorf("failed to read WAL records: %w", err)
	}

	if len(recordsByTxn) == 0 {
		return nil // No records to recover
	}

	logger.Infof("🔄 Check %d transactions from WAL...\n", len(recordsByTxn))

	// Process each transaction
	hasRepayed := false
	for txnID, records := range recordsByTxn {
		if e.txnManager.needRepay(txnID) {
			if err := e.replayTransaction(txnID, records); err != nil {
				logger.Errorf("Warning: failed to replay transaction %d: %v\n", txnID, err)
				os.Exit(1)
			}
			hasRepayed = true
			logger.Infof(" ✅ Replayed record %+v.\n", records)
		}
	}
	if hasRepayed {
		logger.Infof("✅ WAL recovery completed. Next transaction ID: %d\n", e.metadata.NextTxnID)
	} else {
		logger.Info("✅ WAL recovery completed. No transactions to replay.")
	}

	return nil
}
