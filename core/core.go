package core

import (
	"bufio"
	"hash/adler32"
	"io"
	"os"
)

type Block struct {
	Offset uint64
	Data   []byte
}

func GetChecksums(f *os.File, blockSize int) ([]uint32, error) {

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

	hashList := make([]uint32, l)

	for i := 0; ; i++ {
		//n, err := r.Read(buffer[:cap(buffer)])
		//buf = buf[:n]
		n, err := r.Read(buffer)
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
		hashList[i] = sum
	}
	return hashList, nil
}
