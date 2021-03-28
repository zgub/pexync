package core

import (
	"bufio"
	"crypto/sha1"
	"hash/adler32"
	"io"
	"os"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/fs"
)

func GetChecksums(filePath string) (*fs.FileDesc, error) {

	blockSize := viper.GetInt("block_size")
	log.Info().
		Int("using block size", blockSize).
		Msg("initializing")

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buffer := make([]byte, blockSize)
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size()
	stat := fileInfo.Sys().(*syscall.Stat_t)

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
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return nil, err
		}
		sum := adler32.Checksum(buffer)

		log.Debug().
			Int("i", i).
			Int("bytes", n).
			Uint32("sum", sum).
			Msg("checksum")

		hashList[i] = sum
	}
	fd := &fs.FileDesc{
		FilePath: filePath,
		FileName: fileInfo.Name(),
		FileSize: uint64(size),
		Modified: fileInfo.ModTime(),
		Perm:     fileInfo.Mode().Perm(),
		Uid:      stat.Uid,
		Gid:      stat.Gid,
		Sha1:     sha1sh.Sum(nil)[:20],
		Weak:     hashList,
	}
	return fd, nil
}

func Roll(dst *fs.FileDesc, src string) (bool, error) {
	return false, nil
}
