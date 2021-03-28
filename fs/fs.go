package fs

import (
	"os"
	"time"
)

type FileDesc struct {
	FilePath string
	FileName string
	FileSize uint64
	Modified time.Time
	Perm     os.FileMode
	Uid, Gid uint32
	Sha1     []byte
	Weak     []uint32
}

func GetList(path string) ([]*FileDesc, error) {
	return nil, nil
}
