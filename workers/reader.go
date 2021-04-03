package workers

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/zgub/pexync/core"
)

type RollReader struct {
	reader     *io.SectionReader
	receiver   <-chan []*core.Message
	blockSize  int
	sendBuffer []byte
}

func NewRollReader(ctx context.Context, wg *sync.WaitGroup, blockSize int, sr *io.SectionReader, receiver <-chan []*core.Message) *RollReader {
	return &RollReader{
		reader:     sr,
		receiver:   receiver,
		blockSize:  blockSize,
		sendBuffer: make([]byte, blockSize),
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

}
