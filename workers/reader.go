package workers

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

// would be the reallocating of a new bytearray faster?
type dataBuf struct {
	p    int
	data []byte
}

func (b *dataBuf) Reset() {
	b.p = 0
}

type RollReader struct {
	ctx       context.Context
	wg        *sync.WaitGroup
	reader    *io.SectionReader
	receiver  chan<- *core.Message
	blockSize int
	fd        *lfs.FileDesc
	senderID  uuid.UUID
	keep      []byte // circular buffer to 'remember' previous data
	p         int
	dataBuf   []byte
}

func NewRollReader(ctx context.Context, wg *sync.WaitGroup, senderID uuid.UUID, fd *lfs.FileDesc, blockSize int, sr *io.SectionReader, receiver chan<- *core.Message) *RollReader {
	return &RollReader{
		ctx:       ctx,
		wg:        wg,
		senderID:  senderID,
		reader:    sr,
		receiver:  receiver,
		blockSize: blockSize,
		fd:        fd,
		keep:      make([]byte, blockSize),
		dataBuf:   make([]byte, blockSize),
	}
}

func (rr *RollReader) Start() {
	defer rr.wg.Done()
	log.Trace().Msg("starting file reader")

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
		log.Trace().
			Str("file", rr.fd.FileName).
			Uint32("first block match", rr.fd.Weak[0]).
			Send()
		// it matches so jump to next block
		_, err := rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
		core.Fatality(err)
		rr.fd.Matches = append(rr.fd.Matches, 0)
	} else {
		log.Trace().
			Str("file", rr.fd.FileName).
			Uint32("first block did not match", rr.fd.Weak[0]).
			Send()
	}
	// initialize out byte with the first byte of the section
	rr.keep = buf

	var (
		rollSum uint32
		pkt     *core.Message
	)
	var pos int64
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
				fmt.Printf("this error: %s", err.Error())
				panic(err)
			}
			if err == io.EOF {
				// yay, nd of file, ehm section, well this should be addresses
				break
			}
		}
		pos += int64(n)
		log.Trace().
			Str("name", rr.fd.FileName).
			Int("read bytes", n).
			Int("data buf len", len(rr.fd.Data)).
			Msgf("rolling %d / %d", pos, rr.reader.Size())
		// one never knows, but should not be an issue except for the end of file
		buf = buf[:n]
		// now let's feed the hash byte by byte
		for _, b := range buf {
			select {
			case <-rr.ctx.Done():
				return
			default:

			}
			rh.Roll(b)
			rollSum = rh.Sum32()
			// lookup in the remote file hash list
			for remoteHashPos, hash := range rr.fd.Weak {
				if rollSum == hash {
					rr.fd.Matches = append(rr.fd.Matches, remoteHashPos)
					// skip matching bytes
					log.Trace().Msg("found block, skipping")
					rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
					// maybe this could be optimized for subsequent matchin hashes
					break
				}
				// not found, append the oldest ring buffer byte to the packet
				missingByte := rr.pop()
				rr.fd.Data = append(rr.dataBuf, missingByte)
				// check length
				if len(rr.dataBuf) == rr.blockSize {
					pkt = &core.Message{
						Flag: core.DTA,
						File: rr.fd,
						UUID: rr.senderID,
					}
					// send thepackage when full
					err := sendWithTimeout(pkt, rr.receiver)
					core.Fatality(err)
				}
				// store the byte in the circular buffer
				rr.push(b)
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
