package lfs

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

type FileDesc struct {
	IsDir    bool
	FilePath string
	FileName string
	FileSize uint64
	Modified time.Time
	Mode     os.FileMode
	Uid, Gid uint32
	Sha1     []byte
	Weak     []uint32
}

func GetList(path string) ([]*FileDesc, error) {
	var list []*FileDesc
	err := filepath.WalkDir(path, func(path string, entry os.DirEntry, err error) error {

		if err != nil {
			log.Error().
				Err(err).
				Msg("error parsing directory")
			return err
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		stat := info.Sys().(*syscall.Stat_t)

		fileDesc := &FileDesc{
			IsDir:    entry.IsDir(),
			FilePath: path,
			FileName: entry.Name(),
			FileSize: uint64(info.Size()),
			Modified: info.ModTime(),
			Uid:      stat.Uid,
			Gid:      stat.Gid,
		}

		list = append(list, fileDesc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}
