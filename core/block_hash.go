package core

import (
	"bufio"
	"crypto/sha1"
	"hash/adler32"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/lfs"
)

func AddChecksums(fd *lfs.FileDesc) error {
	p := filepath.Join(fd.Prefix, fd.FileName)
	f, err := os.Open(p)
	if err != nil {
		return errors.Wrap(err, "error opening file")
	}
	defer f.Close()

	buffer := make([]byte, fd.BlockSize)
	fileInfo, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "file stat error")
	}
	size := fileInfo.Size()

	sha1sh := sha1.New()
	// func TeeReader(r Reader, w Writer) Reader
	r := io.TeeReader(bufio.NewReader(f), sha1sh)
	if fd.BlockSize == 0 {
		log.Warn().
			Str("path", filepath.Join(fd.Prefix, fd.FileName)).
			Msg("empty file")
		fd.BlockSize = 700
	}
	hashListLen := size / int64(fd.BlockSize)
	if (size % int64(fd.BlockSize)) != 0 {
		hashListLen++
	}

	hashList := make([]uint32, hashListLen)

	for i := 0; ; i++ {
		//n, err := r.Read(buffer[:cap(buffer)])
		//buf = buf[:n]
		n, err := io.ReadFull(r, buffer)
		if n == 0 {
			// is this really necessary?
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "error while reading file")
		}
		sum := adler32.Checksum(buffer)

		hashList[i] = sum
	}

	fd.Sha1 = sha1sh.Sum(nil)[:20]
	fd.Weak = hashList
	log.Trace().
		Str("dst path", filepath.FromSlash(fd.Prefix+"/"+fd.FileName)).
		Int("checksums added", len(hashList)).
		Msg("checksums calculated")
	return nil
}
