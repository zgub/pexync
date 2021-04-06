package workers

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type RollReader struct {
	reader    *io.SectionReader
	receiver  chan<- *core.Message
	blockSize int
	fd        *lfs.FileDesc
	senderID  uuid.UUID
	keep      []byte // circular buffer to 'remember' previous data
	p         int
}

func NewRollReader(ctx context.Context, senderID uuid.UUID, wg *sync.WaitGroup, fd *lfs.FileDesc, blockSize int, sr *io.SectionReader, receiver chan<- *core.Message) *RollReader {
	return &RollReader{
		senderID:  senderID,
		reader:    sr,
		receiver:  receiver,
		blockSize: blockSize,
		fd:        fd,
		keep:      make([]byte, blockSize),
	}
}

func (rr *RollReader) Start() {

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
		rr.fd.Matches = append(rr.fd.Matches, 0)
	}
	// initialize out byte with the first byte of the section
	oldBuf := buf

	var (
		rollSum uint32
		pkt     *core.Message
	)
	// read through the file
	for {
		// fetch blockDize of data
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
				// yay, nd of file, ehm section, well this should be addresses
				break
			}

			// one never knows, but should not be an issue except for the end of file
			buf = buf[:n]
			// now let's feed the hash byte by byte
			for pos, b := range buf {

				rh.Roll(b)
				rollSum = rh.Sum32()
				// lookup in the remote file hash list
				for remoteHashPos, hash := range rr.fd.Weak {
					if rollSum == hash {
						rr.fd.Matches = append(rr.fd.Matches, remoteHashPos)
						// skip matching bytes
						rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
						// maybe this could be optimized for subsequent matchin hashes
						break
					}
					// not found, append the out byte to the packet
					missingByte := oldBuf[pos]
					rr.fd.Data = append(rr.fd.Data, missingByte)
					// check length
					if len(rr.fd.Data) == rr.blockSize {
						pkt = &core.Message{
							Flag: core.DTA,
							File: rr.fd,
							UUID: rr.senderID,
						}
						// send thepackage when full
						err := sendWithTimeout(pkt, rr.receiver)
						core.Fatality(err)
					}

				}
			}

		}

	}

}

// write value to the circular buffer
func (rr *RollReader) push(b byte) {
	rr.keep[rr.p] = b
	rr.p++
	if rr.p == len(rr.keep) {
		// reset
		rr.p = 0
	}
}

// read the oldest value
func (rr *RollReader) pop() byte {
	return rr.keep[rr.p]
}
