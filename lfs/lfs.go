package lfs

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

type State int

const (
	Missing State = iota // no file on the receiver side
	Diff                 // file exists but do not match
	Skip                 // file exists and matches
)

type FileDesc struct {
	State    State
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

func GetList(walkPath, prefix string) ([]*FileDesc, error) {
	//walkPath = prefix + walkPath
	var list []*FileDesc
	log.Trace().
		Str("walk path", walkPath).
		Send()
	err := filepath.WalkDir(walkPath, func(path string, entry os.DirEntry, err error) error {

		if err != nil {
			log.Error().
				Err(err).
				Msg("error parsing directory")
			return err
		}

		/*
			if path == walkPath {
				return filepath.SkipDir
			}
		*/

		log.Trace().
			Str("path", path).
			Str("walk path", walkPath).
			Send()

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
			Mode:     info.Mode(),
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
