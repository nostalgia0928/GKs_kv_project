package bitcask_go

import (
	"bitcask-go/data"
	"sync"
)

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	log_record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
}

func (db *DB) appendLogRecord(LogRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile == nil {

	}
}

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
}
