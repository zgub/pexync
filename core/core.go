package core

import (
	"bufio"
	"crypto/sha1"
	"hash/adler32"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/lfs"
)

func AddChecksums(fd *lfs.FileDesc) error {

	blockSize := viper.GetInt("block_size")
	log.Trace().
		Int("using block size", blockSize).
		Str("file", fd.FileName).
		Send()

	f, err := os.Open(fd.Prefix + "/" + fd.RelPath)
	if err != nil {
		return errors.Wrap(err, "error opening file")
	}
	defer f.Close()

	buffer := make([]byte, blockSize)
	fileInfo, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "file stata error")
	}
	size := fileInfo.Size()

	sha1sh := sha1.New()
	// func TeeReader(r Reader, w Writer) Reader
	r := io.TeeReader(bufio.NewReader(f), sha1sh)

	l := size / int64(blockSize)
	if (size % int64(blockSize)) != 0 {
		l++
	}

	hashList := make([]uint32, l)

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
	return nil
}
