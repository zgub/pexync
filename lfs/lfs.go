package lfs

import (
	"math"
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

var fileStatus = [...]string{
	"MISS",
	"DIFF",
	"SKIP",
}

func (s State) String() string {
	return fileStatus[s]
}

// common errors, lazy to type
const (
	absPathError = "error listing directory - failed while determining the absolute path"
	walkError    = "error listing directory"
)

type FileDesc struct {
	Idx       int32
	State     State
	IsDir     bool
	RelPath   string
	Prefix    string
	FileName  string
	FileSize  uint64
	BlockSize int
	Modified  time.Time
	Mode      os.FileMode
	Uid, Gid  uint32
	Sha1      []byte
	Weak      []uint32
	Matches   []int
	Data      []byte
}

func ParseDir(walkDir string) ([]*FileDesc, error) {
	//walkPath = prefix + walkPath
	var list []*FileDesc

	log.Trace().
		Str("walk dir", walkDir).
		Send()

	// don't do walk over abs path, makes comparing more difficult
	walkDirAbs, err := filepath.Abs(walkDir)
	if err != nil {
		return nil, errors.Wrap(err, absPathError)
	}

	// filepath index to refer tol later
	var idx int32

	// avoid endless recursive deadend
	dest, err := filepath.Abs(viper.GetString("local_destination"))
	if err != nil {
		return nil, errors.Wrap(err, absPathError)
	}

	err = filepath.WalkDir(walkDir, func(path string, entry os.DirEntry, err error) error {

		if err != nil {
			return errors.Wrap(err, walkError)
		}

		// skip destination folder if it's located within the source
		absPath, err := filepath.Abs(path)
		if err != nil {
			return errors.Wrap(err, absPathError)
		}

		// not cheap, but it's not done that often
		if absPath == dest && walkDirAbs != dest {
			log.Trace().
				Str("path", path).
				Str("destination", dest).
				Msg("skipping destination")
			return filepath.SkipDir
		}

		info, err := entry.Info()
		if err != nil {
			return errors.Wrap(err, "file stat info failed")
		}

		stat := info.Sys().(*syscall.Stat_t)

		relPath, err := filepath.Rel(walkDir, path)
		if err != nil {
			return errors.Wrap(err, "failed to determine relative file path")
		}
		prefix := filepath.Dir(absPath)
		log.Trace().
			Str("path", path).
			Str("prefix path", prefix).
			Bool("is dir", entry.IsDir()).
			Msg("parsing")

		if relPath != "." {
			fileDesc := &FileDesc{
				Idx:      idx,
				IsDir:    entry.IsDir(),
				RelPath:  relPath,
				Prefix:   prefix,
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
		return nil, errors.Wrap(err, "error listing directory")
	}
	log.Trace().
		Int("returning filelist size", len(list)).
		Str("walk dir", walkDir).
		Send()
		// increment file index
	idx++
	return list, nil
}

func (fd *FileDesc) SetBlockSize() {
	// fetch the config value, which has priority if changed and remains 700 if filesize is sma;;
	fd.BlockSize = viper.GetInt("block_size")
	// if the file size is big enoigh anf the value is still default
	if fd.FileSize > 490000 && fd.BlockSize == 700 {
		// stolen from rsync doc :)
		sqrt := math.Sqrt(float64(fd.FileSize))
		fd.BlockSize = int(math.Round(sqrt))
		if fd.BlockSize > 131072 {
			fd.BlockSize = 131072
		}
	}

	if fd.FileSize < 700 {
		fd.BlockSize = int(fd.FileSize)
	}

}
