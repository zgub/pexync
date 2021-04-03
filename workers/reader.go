package workers

import (
	"context"
	"io"
	"sync"

	"github.com/zgub/pexync/core"
)

type RollReader struct {
	reader    *io.SectionReader
	receiver  <-chan []*core.Message
	blockSize int
	buffer    []byte
}

func NewRollReader(ctx context.Context, wg *sync.WaitGroup, blockSize int, sr *io.SectionReader, receiver <-chan []*core.Message) *RollReader {
	return &RollReader{
		reader:    sr,
		receiver:  receiver,
		blockSize: blockSize,
		buffer:    make([]byte, blockSize),
	}
}

func (rr *RollReader) Start() {

}
