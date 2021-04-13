package workers

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

const (
	HashNotFound int64 = -1
)

type RollReader struct {
	ctx      context.Context
	receiver chan<- *core.Message
	inbox    <-chan *core.Message
	senderID uuid.UUID
	ring     []byte // ring buffer won't do, nor bytes.Buffer
	p        int
	hMap     map[uint32]int
}

func NewRollReader(ctx context.Context, senderID uuid.UUID, sr *io.SectionReader, inbox <-chan *core.Message, receiver chan<- *core.Message) *RollReader {
	return &RollReader{
		ctx:      ctx,
		inbox:    inbox,
		receiver: receiver,
	}
}

// todo implement map index
func (w *RollReader) Start() error {
	log.Trace().Msg("starting file reader")

	var (
		done      bool // default false
		skipCount int
	)

	for !done {
		// wait for file (or a section)
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("local receiver closing, context done")
			done = true
			break
		case msg := <-w.inbox:
			path := msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName
			// this could be possibly optimized in order to save file descriptors
			f, err := os.Open(path)
			if err != nil {
				return errors.Wrap(err, "unable to open file for hash comparison")
			}
			r := io.ReaderAt(f)
			sr := io.NewSectionReader(r, msg.FileDesc.Offset, msg.FileDesc.Limit)
			br := bufio.NewReader(sr)
			buf := make([]byte, msg.FileDesc.BlockSize)

			// create a hash map for faster lookup
			w.hMap = make(map[uint32]int)
			for i, h := range msg.FileDesc.Weak {
				w.hMap[h] = i
			}

			n, err := io.ReadFull(br, buf)
			if n == 0 {
				if err == nil {
					return errors.Wrapf(err, "%s - hash comparator receiver 0 bytes while readind the file", msg.FileDesc.FileName)
				} else if err != io.EOF {
					return errors.Wrapf(err, "%s error reading file while comparing hashes", msg.FileDesc.FileName)
				}
				if err == io.EOF {
					return nil
				}
			}

			// initialize the roll buffer by writting the first block
			rh := core.Pour()
			_, err = rh.Write(buf)
			if err != nil {
				return errors.Wrap(err, "error writing to roll window")
			}

			hIndex := w.lookup(rh.Sum32())
			if err != nil {
				return errors.Wrap(err, "hash lookup failure")
			}
			dd := new(lfs.DataDesc)
			if hIndex != HashNotFound {
				err = dd.WriteIndex(hIndex)
				if err != nil {
					return errors.Wrap(err, "roll hash calculation failed")
				}
			}
			// store the old buf
			w.ring = buf

			// now continue through the rest of the file secion
			for {
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

				// if for the last time we've found a matching block, let's read another whole block
				if hIndex != HashNotFound {
					skipCount++
					// lets read full blocksize, because the lat one matched
					rh.Reset()
					_, err := rh.Write(buf)
					if err != nil {
						return errors.Wrap(err, "error writing to roll window")
					}
					//log.Trace().Msg("read a wrote whole block")
					hIndex = w.lookup(rh.Sum32())
					if hIndex != HashNotFound {
						// again matching block, next!
						err = dd.WriteIndex(hIndex)
						if err != nil {
							return errors.Wrap(err, "roll hash calculation failed")
						}
						continue
					}
				}
				// last buffer was no luck, go byte by byte
				for _, b := range buf {
					rh.Roll(b)
					// lookup in the remote file hash list
					hIndex = w.lookup(rh.Sum32())
					if hIndex != HashNotFound {
						err = dd.WriteIndex(hIndex)
						if err != nil {
							return errors.Wrap(err, "roll hash calculation failed")
						}
						continue
					}
					// no luck this time
					// first add the oldes byte to the send buffer
					dd.WriteByte(w.pop())
					// check the sendBuf size and send it eventually
					if dd.Len() > msg.FileDesc.BlockSize {
						// append is not thread safe!
						msg := &core.Message{
							Flag:     core.DTA,
							FileDesc: msg.FileDesc,
							DataDesc: dd,
						}
						err = sendWithTimeout(msg, w.receiver)
						if err != nil {
							return errors.Wrap(err, "error sending data")
						}
					}
					// then push the new into the ring buffer
					w.push(b)
				}
			}
		}
	}
	return nil
}

// write value to the circular buffer
func (rr *RollReader) push(b byte) {
	// (over)write
	rr.ring[rr.p] = b
	// increment
	rr.p++
	// reset if overflow
	if rr.p == len(rr.ring) {
		rr.p = 0
	}
}

// read the oldest value
func (rr *RollReader) pop() byte {
	return rr.ring[rr.p]
}

func (w *RollReader) lookup(sum uint32) int64 {

	if hIndex, ok := w.hMap[sum]; ok {
		// found
		return int64(hIndex)
	}
	return HashNotFound
}

type HashReader struct {
	ctx   context.Context
	inbox <-chan *core.Message
}

func NewHashreader(ctx context.Context, inbox <-chan *core.Message) *HashReader {
	return &HashReader{
		ctx:   ctx,
		inbox: inbox,
	}
}

func (w *HashReader) Start() error {
	var done bool
	for !done {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("hash reader closing, context done")
			done = true
		case msg := <-w.inbox:
			if msg.Flag == core.FIN {
				log.Debug().
					Msg("hash reader received FIN")
				done = true
			} else {
				err := core.AddChecksums(msg.FileDesc)
				if err != nil {
					return errors.Wrap(err, "error calculating initial hash array")
				}
			}
		}
	}
	return nil
}
