package lsm

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"kv/pkg/common"
	"kv/pkg/utils"
	"kv/pkg/wal"
)

// IsolationLevel 表示事务隔离级别
type IsolationLevel int

const (
	// ReadUncommitted  允许脏读allows dirty reads
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted prevents dirty reads禁止脏读
	ReadCommitted
	// RepeatableRead prevents dirty and non-repeatable reads禁止脏读和不可重复读
	RepeatableRead
	// Serializable prevents dirty reads, non-repeatable reads, and phantom reads禁止脏读、不可重复读和幻读
	Serializable
)

// TransactionState represents the state of a transaction表示交易状态
type TransactionState int

const (
	// TxnActive transaction is active事务处于活动状态
	TxnActive TransactionState = iota
	// TxnCommitted transaction is committed事务已提交
	TxnCommitted
	// TxnAborted transaction is aborted/rolled back事务已中止/回滚
	TxnAborted
)

// Transaction 事务 代表数据库事务
type Transaction struct {
	id         uint64
	state      TransactionState // 状态
	isolation  IsolationLevel   // 事务隔离级别
	startTime  time.Time
	commitTime time.Time
	readTxnID  uint64 // Transaction ID used for reads (for snapshot isolation)
	manager    *TransactionManager
	mu         sync.RWMutex

	// Transaction-specific data storage for non READ_UNCOMMITTED levels
	tempMap     map[string]string          // Temporary storage for uncommitted writes
	readMap     map[string]*ReadRecord     // Read history for REPEATABLE_READ and SERIALIZABLE
	rollbackMap map[string]*RollbackRecord // Rollback information for READ_UNCOMMITTED
	operations  []*wal.Record              // WAL operations for this transaction
}

// TransactionManager 管理数据库事务
type TransactionManager struct {
	engine        *Engine
	activeTxns    map[uint64]*Transaction //活跃事务集合
	committedTxns map[uint64]*Transaction //已提交事务集合
	mu            sync.RWMutex
	config        *TransactionConfig
}

// TransactionConfig holds transaction manager configuration包含事务管理器配置
type TransactionConfig struct {
	// DefaultIsolationLevel 是新事务的默认隔离级别
	DefaultIsolationLevel IsolationLevel
	// MaxActiveTxns 是活动事务的最大数量
	MaxActiveTxns int
	// TxnTimeout 是事务的超时时间
	TxnTimeout time.Duration
	// CleanupInterval 是清理已提交事务的间隔时间
	CleanupInterval time.Duration
}

// DefaultTransactionConfig 返回默认事务配置
func DefaultTransactionConfig() *TransactionConfig {
	return &TransactionConfig{
		DefaultIsolationLevel: ReadCommitted,
		MaxActiveTxns:         1000,
		TxnTimeout:            30 * time.Second,
		CleanupInterval:       60 * time.Second,
	}
}

// NewTransactionManager 创建一个新的事务管理器
func NewTransactionManager(engine *Engine, config *TransactionConfig) *TransactionManager {
	if config == nil {
		config = DefaultTransactionConfig()
	}

	mgr := &TransactionManager{
		engine:        engine,
		activeTxns:    make(map[uint64]*Transaction),
		committedTxns: make(map[uint64]*Transaction),
		config:        config,
	}
	mgr.loadTxnStatus()

	return mgr
}

// Begin 启动一个具有默认隔离级别的新事务
func (m *TransactionManager) Begin() (*Transaction, error) {
	return m.BeginWithIsolation(m.config.DefaultIsolationLevel)
}

// BeginWithIsolation 会启动一个具有指定隔离级别的新事务
func (m *TransactionManager) BeginWithIsolation(isolation IsolationLevel) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否超过最大活跃事物量
	if len(m.activeTxns) >= m.config.MaxActiveTxns {
		return nil, utils.ErrTooManyActiveTxns
	}

	txnID := atomic.AddInt64(&m.engine.metadata.NextTxnID, 1) - 1
	readTxnID := atomic.LoadInt64(&m.engine.metadata.GlobalReadTxnID)

	txn := &Transaction{
		id:        txnID,
		state:     TxnActive,
		isolation: isolation,
		startTime: time.Now(),
		readTxnID: readTxnID,
		manager:   m,
		// Initialize transaction-specific maps
		tempMap:     make(map[string]string),
		readMap:     make(map[string]*ReadRecord),
		rollbackMap: make(map[string]*RollbackRecord),
		operations:  []*wal.Record{wal.NewCreateRecord(txnID)},
	}

	m.activeTxns[txnID] = txn
	return txn, nil
}

// GetTransaction 通过 ID 返回事物
func (m *TransactionManager) GetTransaction(txnID uint64) (*Transaction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 如果 activeTxns[txnID]存在
	if txn, exists := m.activeTxns[txnID]; exists {
		return txn, true
	}

	if txn, exists := m.committedTxns[txnID]; exists {
		return txn, true
	}

	return nil, false
}

// GetActiveTransactionCount 返回活跃交易的数量
func (m *TransactionManager) GetActiveTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.activeTxns)
}

// GetCommittedTransactionCount 返回已提交交易的数量
func (m *TransactionManager) GetCommittedTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.committedTxns)
}

// GetNextTxnID 返回将要分配的下一个事务 ID
func (m *TransactionManager) GetNextTxnID() uint64 {
	return atomic.LoadUint64(&m.engine.metadata.NextTxnID)
}

// 更新已冲洗事物
func (m *TransactionManager) updateFlushedTxn(txnID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove flushed transactions
	delete(m.committedTxns, txnID)
}

// 同步事物状态 将当前活跃事务的状态持久化到磁盘
func (m *TransactionManager) syncTxnStatus() error {
	data, err := json.MarshalIndent(m.activeTxns, "", "  ")
	if err != nil {
		log.Println("Failed to marshal active transactions: %v", err)
		return err
	}
	filePath := filepath.Join(m.engine.dataDir, common.CommittedTxnFile)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write committed transactions file: %w", err)
	}
	return nil
}

// 读已加载的事务信息到m
func (m *TransactionManager) loadTxnStatus() error {
	data, err := os.ReadFile(filepath.Join(m.engine.dataDir, common.CommittedTxnFile))
	if err != nil {
		return fmt.Errorf("failed to read committed transactions file: %w", err)
	}
	var records map[uint64]*Transaction
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("failed to unmarshal committed transactions: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedTxns = records
	return nil
}

// 判断是否提交
func (m *TransactionManager) needRepay(txnID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exist := m.committedTxns[txnID]
	return exist
}

// GetactiveTxnIDs 获取当前所有 “活跃事务” 的 ID 集
func (m *TransactionManager) GetactiveTxnIDs() map[uint64]struct{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	activeIDs := make(map[uint64]struct{})
	for id := range m.activeTxns {
		activeIDs[id] = struct{}{}
	}
	return activeIDs
}

// Close 关闭-存到磁盘里
func (m *TransactionManager) Close() error {
	return m.syncTxnStatus()
}

// ID returns the transaction ID
func (t *Transaction) ID() uint64 {
	return t.id
}

// State returns the transaction state
func (t *Transaction) State() TransactionState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// IsolationLevel 返回事务隔离级别
func (t *Transaction) IsolationLevel() IsolationLevel {
	return t.isolation
}

// StartTime returns the transaction start time 开始时间
func (t *Transaction) StartTime() time.Time {
	return t.startTime
}

// CommitTime returns the transaction commit time (only valid for committed transactions)返回事务提交时间（仅对已提交的事务有效）
func (t *Transaction) CommitTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.commitTime
}

// ReadTxnID 返回用于读取的事务 ID（快照）
// 对于活动事务，使用自身的 ID 查看自身的写入
// 对于已提交/已中止的事务，使用原始快照
func (t *Transaction) ReadTxnID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.state == TxnActive {
		// Active transactions can read their own writes
		return t.id
	}

	// Use snapshot for non-active transactions  对非活跃交易使用快照
	return t.readTxnID
}

// WriteTxnID 返回用于写入的事务 ID。
func (t *Transaction) WriteTxnID() uint64 {
	return t.id
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 必须是活跃表
	if t.state != TxnActive {
		return utils.ErrTransactionNotActive
	}

	// 不同隔离级别
	switch t.isolation {
	case ReadCommitted:
		// READ_UNCOMMITTED：数据已写入，只需添加提交记录   读已提交
	default:
		// 其他隔离级别：需要冲突检测和批量应用
		if err := t.detectConflicts(); err != nil {
			// 检测到冲突，中止交易
			t.state = TxnAborted
			t.manager.mu.Lock()
			delete(t.manager.activeTxns, t.id)
			t.manager.mu.Unlock()
			return err
		}
	}

	//添加提交记录
	t.operations = append(t.operations, wal.NewCommitRecord(t.id))

	// Write all operations to WAL
	if err := t.manager.engine.wal.Log(t.operations, true); err != nil {
		return fmt.Errorf("failed to write transaction operations to WAL: %w", err)
	}
	// Apply changes to database将更改应用到数据库
	if err := t.applyChanges(); err != nil {
		return err
	}
	// 将事务标记为已提交
	t.state = TxnCommitted
	t.commitTime = time.Now()
	// 从活动事务切换到已提交事务
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t.id)
	t.manager.committedTxns[t.id] = t

	// Update global read transaction ID for new snapshots 更新新快照的全局读取事务 ID
	atomic.StoreUint64(&t.manager.engine.metadata.GlobalReadTxnID, t.id)
	t.manager.mu.Unlock()

	// 强制冲洗以确保耐用性
	return t.manager.engine.Flush()
}

// 回滚操作会撤销事务
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != TxnActive {
		return utils.ErrTransactionNotActive
	}

	// 根据隔离级别处理回滚
	if t.isolation == ReadUncommitted {
		// READ_UNCOMMITTED：需要主动恢复先前的值
		if err := t.rollbackChanges(); err != nil {
			return err
		}

		// Add rollback record to WAL
		t.operations = append(t.operations, wal.NewRollbackRecord(t.id))
	}

	// 对于其他隔离级别，数据位于 tempMap 中，将被丢弃

	// 将事务标记为已中止
	t.state = TxnAborted

	// Remove from active transactions
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t.id)
	t.manager.mu.Unlock()

	return nil
}

// IsActive 如果事务处于活动状态，则 IsActive 返回 true
func (t *Transaction) IsActive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnActive
}

// IsCommitted  如果事务已提交，则 IsCommitted 返回 true
func (t *Transaction) IsCommitted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnCommitted
}

// IsAborted // 如果事务已中止，则 IsAborted 返回 true
func (t *Transaction) IsAborted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnAborted
}

// ReadRecord 表示隔离级别的读取操作记录
type ReadRecord struct {
	Value  string
	TxnID  uint64
	Exists bool
}

// RollbackRecord 表示 READ_UNCOMMITTED 的回滚信息u
type RollbackRecord struct {
	Value  string
	TxnID  uint64
	Exists bool
}

// detectConflicts 函数检查 REPEATABLE_READ 和 SERIALIZABLE 隔离级别的写入冲突
func (t *Transaction) detectConflicts() error {
	if t.isolation != RepeatableRead && t.isolation != Serializable {
		return nil // No conflict detection needed
	}

	// 检查 tempMap 中的每个键是否存在冲突
	for key := range t.tempMap {
		// 检查后续交易是否修改了此键
		_, exists, err := t.manager.engine.GetWithTxnID(key, 0) // Get without transaction filtering
		if err != nil {
			return err
		}
	}
	if exists {
		// 获取上次修改此键的事务 ID
		// 这是一个简化的方法 - 在完整实现中，
		// 我们需要跟踪每次修改的事务 ID
		// 目前，我们使用基于全局读取事务 ID 的简单启发式方法
		globalReadTxnID := atomic.LoadUint64(&t.manager.engine.metadata.GlobalReadTxnID)
		if globalReadTxnID > t.id {
			// A later transaction has committed changes, potential conflict
			return utils.ErrTransactionConflict
		}
	}

	return nil
}

// applyChanges 将临时更改应用到数据库。
func (t *Transaction) applyChanges() error {
	if t.isolation == ReadUncommitted {
		return nil // No change application needed for other isolation levels
	}

	for key, value := range t.tempMap {
		if value == "" {
			// Empty string marks deletion
			if err := t.manager.engine.DeleteWithTxn(key, t.id); err != nil {
				return err
			}
		} else {
			// Regular put operation
			if err := t.manager.engine.PutWithTxnID(key, value, t.id); err != nil {
				return err
			}
		}
	}
	// add a marker to denote end of transaction
	if err := t.manager.engine.PutWithTxnID("", "", t.id); err != nil {
		return err
	}

	return nil
}

// rollbackChanges rolls back changes for READ_UNCOMMITTED transactions
func (t *Transaction) rollbackChanges() error {
	if t.isolation != ReadUncommitted {
		return nil // No rollback needed for other isolation levels
	}

	// Restore previous values
	for key, record := range t.rollbackMap {
		if record.Exists {
			// Restore previous value
			if err := t.manager.engine.PutWithTxnID(key, record.Value, record.TxnID); err != nil {
				return err
			}
		} else {
			// Key didn't exist before, delete it
			if err := t.manager.engine.DeleteWithTxn(key, t.id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) GetTxnID() uint64 {
	return t.id
}
