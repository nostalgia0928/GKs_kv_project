package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New(" The key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("DataFile not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMergeIsProgress        = errors.New("files are merging")
)
