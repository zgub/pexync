package fs

import (
	"time"
)

type FileDesc struct {
	FilePath string
	FileSize uint64
	Modified time.Time
	Sha1     []byte
	Weak     []uint32
}

func GetList(path string) ([]*FileDesc, error) {
	return nil, nil
}
