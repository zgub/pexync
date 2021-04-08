package workers

import (
	"bufio"
	"context"
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type RollReader struct {
	ctx       context.Context
	reader    *io.SectionReader
	receiver  chan<- *core.Message
	blockSize int
	fd        *lfs.FileDesc
	senderID  uuid.UUID
	keep      []byte // circular buffer to 'remember' previous data
	p         int
	dataBuf   []byte
}

func NewRollReader(ctx context.Context, senderID uuid.UUID, fd *lfs.FileDesc, blockSize int, sr *io.SectionReader, receiver chan<- *core.Message) *RollReader {
	return &RollReader{
		ctx:       ctx,
		senderID:  senderID,
		reader:    sr,
		receiver:  receiver,
		blockSize: blockSize,
		fd:        fd,
		keep:      make([]byte, blockSize),
		dataBuf:   make([]byte, blockSize),
	}
}

func (rr *RollReader) Start() error {
	log.Trace().Msg("starting file reader")

	// buffered "should" be better
	br := bufio.NewReader(rr.reader)
	buf := make([]byte, rr.blockSize)

	var (
		skip      bool // default false
		skipCount int
	)

	// initial data for boll hash buffer initialization
	n, err := io.ReadFull(br, buf)
	if n == 0 {

		if err == nil {
			return errors.New("read 0 bytes")
		} else if err != io.EOF {
			return err
		}
		if err == io.EOF {
			return nil
		}
	}

	// initilaize the roll hash
	rh := core.Pour()
	_, err = rh.Write(buf)
	if err != nil {
		return errors.Wrap(err, "error writing to roll window")
	}

	skip, err = rr.lookup(rh)
	if err != nil {
		errors.Wrap(err, "lookup error")
	}
	rr.fd.Matches = append(rr.fd.Matches, 0)

	// initialize out byte with the first byte of the section
	rr.keep = buf

	// read through the file
	for {
		// fetch blockDize of data
		n, err := io.ReadFull(br, buf)
		if n == 0 {

			if err == nil {
				return errors.New("read 0 bytes")
			} else if err != io.EOF {
				return errors.Wrap(err, "error reading file")
			}
			if err == io.EOF {
				break
			}
		}

		buf = buf[:n]

		// last time we've found a matching block, let's read another whole block
		if skip {
			skipCount++
			// lets read full blocksize, because the lat one matched
			rh.Reset()
			_, err := rh.Write(buf)
			if err != nil {
				return errors.Wrap(err, "error writing to roll window")
			}
			//log.Trace().Msg("read a wrote whole block")
			skip, err = rr.lookup(rh)
			if err != nil {
				return errors.Wrap(err, "lookup error")
			}
			if skip {
				// again matching block, next!
				continue
			}
		}

		// last buffer - no luck! go byte by byte
		for _, b := range buf {
			rh.Roll(b)
			// lookup in the remote file hash list
			skip, err = rr.lookup(rh)
			if err != nil {
				return errors.Wrap(err, "lookup error")
			}
			if skip {
				break
			}
			// no luck again, push it into send buffer
			rr.push(b)
		}

	}
	log.Info().
		Int("skipped", skipCount).
		Str("name", rr.fd.FileName).
		Int("block count", len(rr.fd.Weak)).
		Msg("report")

	return nil
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

func (rr *RollReader) lookup(rh *core.Radler32) (bool, error) {
	rollSum := rh.Sum32()
	for pos, hash := range rr.fd.Weak {
		if rollSum == hash {
			rr.fd.Matches = append(rr.fd.Matches, pos)
			// skip matching bytes
			//log.Trace().Msg("found block, skipping")
			//rr.reader.Seek(int64(rr.blockSize), io.SeekCurrent)
			// maybe this could be optimized for subsequent matchin hashes
			return true, nil
		}
		// not found, append the oldest ring buffer byte to the packet
		rr.dataBuf = append(rr.dataBuf, rr.pop())
		// check length
		if len(rr.dataBuf) == rr.blockSize {
			pkt := &core.Message{
				Flag: core.DTA,
				File: rr.fd,
				UUID: rr.senderID,
			}
			// send thepackage when full
			err := sendWithTimeout(pkt, rr.receiver)
			if err != nil {
				return false, err
			}
			rr.dataBuf = make([]byte, rr.blockSize)
		}
		// store the byte in the circular buffer
	}
	return false, nil
}
