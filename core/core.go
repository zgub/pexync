package core

import (
	"bufio"
	"hash/adler32"
	"io"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/fs"
)

type Block struct {
	Offset uint64
	Data   []byte
}

func GetChecksums(filePath string, blockSize int) (*fs.FileDesc, error) {

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	buffer := make([]byte, blockSize)
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size()

	l := size / int64(blockSize)
	if (size % int64(blockSize)) != 0 {
		l++
	}
	log.Debug().
		Int("block size", blockSize).
		Int64("file size", size).
		Int64("number of chunks", l).
		Msg("GetChecksums counting chunks")

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
		/*
			log.Debug().
				Int("i", i).
				Int("bytes", n).
				Uint32("sum", sum).
				Msg("checksum")
		*/
		hashList[i] = sum
	}
	fd := &fs.FileDesc{
		FilePath: filePath,
		FileSize: uint64(size),
		Modified: fileInfo.ModTime(),
		Mode:     fileInfo.Mode(),
		Weak:     hashList,
	}
	return fd, nil
}
