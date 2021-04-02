package core

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"hash/adler32"
	"io"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/lfs"
)

func AddChecksums(fd *lfs.FileDesc) error {

	blockSize := viper.GetInt("block_size")
	log.Info().
		Int("using block size", blockSize).
		Msg("initializing")

	f, err := os.Open(fd.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	buffer := make([]byte, blockSize)
	fileInfo, err := f.Stat()
	if err != nil {
		return err
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
			return err
		}
		sum := adler32.Checksum(buffer)

		hashList[i] = sum
	}

	fd.Sha1 = sha1sh.Sum(nil)[:20]
	fd.Weak = hashList
	return nil
}

func Roll(fd *lfs.FileDesc, src string) ([]uint32, error) {
	log.Debug().
		Msg("Rolling")
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	blockSize := viper.GetInt("block_size")
	defer f.Close()
	rollBuff := make([]byte, blockSize)
	r := io.Reader(f)
	//br := bufio.NewReader(r)

	var match, nomatch uint64

	// let's do it, read thw whole file in ~ block size sized chunks first
	rh := Pour()
	n, err := r.Read(rollBuff)
	if n == 0 && err == io.EOF {
		return nil, errors.New("empty file")
	}
	rh.Write(rollBuff)
	// window initialized
	var pos uint64
	for {
		n, err := r.Read(rollBuff)
		if n == 0 {
			if err == nil {
				continue
			}
		}
		if err == io.EOF {
			break
		}

		// roll through this buffer
		for _, b := range rollBuff {
			rh.Roll(b)
			rSum := rh.Sum32()
			for _, sum := range fd.Weak {
				if rSum == sum {
					match++
					break
				} else {
					nomatch++
				}
			}
		}
		if pos%10024 == 0 {
			log.Info().
				Uint64("read [MiB]", pos/1024)
		}

	}

	log.Info().
		Uint64("mached", match).
		Uint64("didn't match", nomatch).
		Int("fd len", len(fd.Weak)).
		Uint64("last position", pos).
		Msg("result")
	return nil, nil
}
