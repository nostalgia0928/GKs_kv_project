package fio

const DataFilePerm = 0644 //用于file_go文件中的NewFileIOManager

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)

	//持久化数据
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
