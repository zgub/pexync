package fs

import (
	"os"
	"time"
)

type FileDesc struct {
	FilePath string
	FileSize uint64
	Modified time.Time
	Mode     os.FileMode
	Sha1     []byte
	Weak     []uint32
}

func GetList(path string) ([]*FileDesc, error) {
	return nil, nil
}
