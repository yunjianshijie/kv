package common

// KVPair 表示批量操作的键值对
type KVPair struct {
	Key   string
	Value string
}

var CommittedTxnFile = "committed_txns.json"
