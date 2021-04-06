package workers

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type RollReader struct {
	reader     *io.SectionReader
	receiver   <-chan *core.Message
	blockSize  int
	sendBuffer []byte
	fd         *lfs.FileDesc
}

func NewRollReader(ctx context.Context, wg *sync.WaitGroup, fd *lfs.FileDesc, blockSize int, sr *io.SectionReader, receiver <-chan *core.Message) *RollReader {
	return &RollReader{
		reader:     sr,
		receiver:   receiver,
		blockSize:  blockSize,
		sendBuffer: make([]byte, blockSize),
		fd:         fd,
	}
}

func (rr *RollReader) Start() {

	// position within section
	var pos int64

	// buffered "should" be better
	br := bufio.NewReader(rr.reader)
	buf := make([]byte, rr.blockSize)

	// initial data for boll hash buffer initialization
	n, err := io.ReadFull(br, buf)
	if n == 0 && err == io.EOF {
		core.Fatality(err)
	}

	// initilaize the roll hash
	rh := core.Pour()
	rh.Write(buf)

	// check the first block already
	if rh.Sum32() == rr.fd.Weak[0] {
		// it matches so jump to next block
		_, err := rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
		core.Fatality(err)
	}

	var rSum uint32
	for ; ; pos++ {
		n, err := io.ReadFull(br, buf)
		if n == 0 {

			if err == nil {
				log.Info().
					Msg("read zero bytes")
					// well, that's cute, let's try again
				continue
			} else if err != io.EOF {
				// poor error handling :-/
				core.Fatality(err)
			}
			if err == io.EOF {
				// yay, nd of file, ehm section
				break
			}

			// one never knows
			buf = buf[:n]
			// now let's feed the hash byte by byte
			for bpos, b := range buf {
				rh.Roll(b)
				rSum = rh.Sum32()
				// lookup
				for rPos, hash := range rr.fd.Weak {

				}
			}

		}

	}

}
