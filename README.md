# kv

## 项目简介
kv是一个基于 

## 核心特性

* 持久化存储: 通过 WAL
* MVCC 事务支持：提供快照隔离级别的配料控制engine_stats.go:36-40
* 多级压缩：平衡读写放大，维持查询性能
* 布隆过滤器优化：加速负查询，减少磁盘I/O
* 块存储：engine.go:29

## 架构组件
* 内存表：
* 沃尔：
* SST 文件: 磁盘上的不可变排序字符串表
* LevelManager : 管理多系统SST文件和压缩策略
* BlockCache : 数据块 LRU 缓存
* 事务管理器：MVCC

## 快速开始
```c
// 创建引擎实例
cfg := config.DefaultConfig()  
engine, err := lsm.NewEngine(cfg, "/path/to/data")  
if err != nil {  
log.Fatal(err)  
}  
defer engine.Close()

// 写入数据  
err = engine.Put([]byte("key"), []byte("value"))

// 读取数据  
value, err := engine.Get([]byte("key"))

// 删除数据  
err = engine.Delete([]byte("key"))
```

## 项目结构
```
pkg/  
├── lsm/          # LSM 引擎核心逻辑  
├── memtable/     # 内存表实现  
├── wal/          # 预写日志  
├── sst/          # SST 文件管理  
├── cache/        # 块缓存  
├── config/       # 配置管理  
└── utils/        # 工具函数  
```

数据流
写入路径：数据首先写入WAL和MemTable

读取路径：按顺序查

后台任务: 发动机运行多engine.go:42-44

统计信息
engine_stats.go:11-33

元数据持久化
engine_stats.go:42-53

