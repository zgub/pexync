package workers

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type RollReader struct {
	reader     *io.SectionReader
	receiver   <-chan []*core.Message
	blockSize  int
	sendBuffer []byte
	fd         *lfs.FileDesc
}

func NewRollReader(ctx context.Context, wg *sync.WaitGroup, fd *lfs.FileDesc, blockSize int, sr *io.SectionReader, receiver <-chan []*core.Message) *RollReader {
	return &RollReader{
		reader:     sr,
		receiver:   receiver,
		blockSize:  blockSize,
		sendBuffer: make([]byte, blockSize),
		fd:         fd,
	}
}

func (rr *RollReader) Start() {
	br := bufio.NewReader(rr.reader)
	buff := make([]byte, rr.blockSize)
	n, err := io.ReadFull(br, buff)
	if n == 0 && err == io.EOF {
		core.Fatality(err)
	}
	rh := core.Pour()
	rh.Write(buff)

	if rh.Sum32() == rr.fd.Weak[0] {
		_, err := rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
		core.Fatality(err)
	}

}
