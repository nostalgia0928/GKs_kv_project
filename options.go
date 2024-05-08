package bitcask_go

import "os"

type Options struct {
	DirPath string

	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	IndexType IndexerType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint

	SyncWrites bool
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1

	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 10124 * 1024,
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
