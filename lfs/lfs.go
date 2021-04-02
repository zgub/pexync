package lfs

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
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

func GetList(walkDir string) ([]*FileDesc, error) {
	//walkPath = prefix + walkPath
	var list []*FileDesc

	// don't do walk over abs path, makes comparinf more difficult
	walkDirAbs, err := filepath.Abs(walkDir)
	if err != nil {
		return nil, err
	}
	// avoid endless recursive deadend
	dest, err := filepath.Abs(viper.GetString("local_destination"))
	if err != nil {
		return nil, err
	}
	err = filepath.WalkDir(walkDir, func(path string, entry os.DirEntry, err error) error {

		if err != nil {
			log.Error().
				Err(err).
				Msg("error parsing directory")
			return err
		}

		// skip destination folder if it's located within the source
		pathAbs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		// not cheap, but it's not done that often
		if pathAbs == dest && walkDirAbs != dest {
			log.Trace().
				Str("path", path).
				Str("destination", dest).
				Msg("skipping destination")
			return filepath.SkipDir
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		stat := info.Sys().(*syscall.Stat_t)

		relPath, err := filepath.Rel(walkDir, path)
		if err != nil {
			return errors.WithMessage(err, "determinign relative path")
		}

		if path != "." {
			fileDesc := &FileDesc{
				IsDir:    entry.IsDir(),
				FilePath: relPath,
				FileName: entry.Name(),
				FileSize: uint64(info.Size()),
				Modified: info.ModTime(),
				Mode:     info.Mode(),
				Uid:      stat.Uid,
				Gid:      stat.Gid,
			}

			list = append(list, fileDesc)
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "GetList")
	}
	log.Trace().
		Int("returning filelist size", len(list)).
		Str("walk dir", walkDir).
		Send()
	return list, nil
}
