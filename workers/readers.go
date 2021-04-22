package workers

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

const (
	HashNotFound int64 = -1
)

// RollReader reads a file, compares the data witha hash table and send either data or indexes
type RollReader struct {
	ctx                       context.Context
	receiver                  chan<- *core.Message
	inbox                     <-chan *core.Message
	senderID                  uuid.UUID
	ring                      []byte // ring buffer won't do, nor bytes.Buffer
	p                         int
	hMap                      map[uint32]int
	indexCnt, dataCnt, msgCnt int64
}

func NewRollReader(ctx context.Context, inbox <-chan *core.Message, receiver chan<- *core.Message) *RollReader {
	return &RollReader{
		ctx:      ctx,
		inbox:    inbox,
		receiver: receiver,
	}
}

func (w *RollReader) Start() error {

	for {
		// wait for file (or a section)
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("roll reader closing, context done")
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.RSQ: // read sequence
				log.Debug().
					Str("filename", msg.FileDesc.FileName).
					Msgf("file received by comparator worker")
				err := w.roll(msg)
				if err != nil {
					return errors.Wrap(err, "roll hash reader failed")
				}
				log.Debug().
					Str("file name", msg.FileDesc.FileName).
					Int64("indexes", w.indexCnt).
					Int64("data", w.dataCnt).
					Int64("messages", w.msgCnt).
					Msg("roll stats")
			case core.FIN:
				log.Trace().
					Msg("file comparator received FIN")
				return nil
			default:
				return errors.New("unknown message received")
			}
		}
	}
}

func (w *RollReader) roll(msg *core.Message) error {
	path := msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName
	// this could be possibly optimized in order to save file descriptors
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "unable to open file for hash comparison")
	}
	defer f.Close()
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
	br := bufio.NewReader(sr)
	buf := make([]byte, msg.FileDesc.BlockSize)

	// create a hash map for faster lookup
	w.hMap = make(map[uint32]int)
	if len(msg.FileDesc.Weak) == 0 {
		log.Warn().Msg("WTH")
	}
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
			log.Trace().
				Msg("roll reader - EOF - while reading first buffer")
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
	// initialize sequence counter
	seq := int64(0)
	dd := lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)
	if hIndex != HashNotFound {
		w.indexCnt++
		err = dd.WriteIndex(hIndex)
		if err != nil {
			return errors.Wrap(err, "roll hash calculation failed")
		}
	}
	// store the old buf
	w.ring = buf

	log.Trace().
		Str("filename", msg.FileDesc.FileName).
		Int64("block size", msg.FileDesc.BlockSize).
		Bool("first block skipped", hIndex != HashNotFound).
		Msg("Rolling")
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
				log.Trace().
					Msg("roll reader - EOF")
				break
			}
		}

		buf = buf[:n]
		// if for the last time we've found a matching block, let's read another whole block
		if hIndex != HashNotFound {
			//skipCnt++
			// lets read full blocksize, because the lat one matched
			rh.Reset()
			_, err := rh.Write(buf)
			if err != nil {
				return errors.Wrap(err, "error writing to roll window")
			}
			//log.Trace().Msg("read a wrote whole block")
			hIndex = w.lookup(rh.Sum32())
			if hIndex != HashNotFound {
				w.indexCnt++
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
				// another match
				err = dd.WriteIndex(hIndex)
				if err != nil {
					return errors.Wrap(err, "roll hash calculation failed")
				}
				w.indexCnt++
				continue
			}
			// no luck this time
			// first add the oldes byte to the send buffer
			dd.WriteByte(w.pop())
			// check the sendBuf size and send it eventually
			if dd.Len() > msg.FileDesc.BlockSize {
				// append is not thread safe!
				nMsg := &core.Message{
					Flag:     core.WSQ,
					FileDesc: msg.FileDesc,
					DataDesc: dd,
				}
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Int64("datadesc len", int64(dd.Len())).
					Int64("block size", msg.FileDesc.BlockSize).
					Msg("roll reader sending data")
				spew.Dump(dd)
				err = sendWithTimeout(nMsg, w.receiver)
				w.msgCnt++
				if err != nil {
					return errors.Wrap(err, "error sending data")
				}
				// new data block
				seq++
				dd = lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)
			}
			// then push the new into the ring buffer
			w.push(b)
			w.dataCnt++
		}
	}
	// don't forget the last data OR if the whole thing was tiny
	//dd.MarkAsLast()
	nMsg := &core.Message{
		Flag:     core.WSQ,
		FileDesc: msg.FileDesc,
		DataDesc: dd,
		Offset:   msg.Offset,
		Limit:    msg.Limit,
	}
	log.Trace().
		Str("filename", msg.FileDesc.FileName).
		Msg("sending remaining data")
	err = sendWithTimeout(nMsg, w.receiver)
	w.msgCnt++
	if err != nil {
		return errors.Wrap(err, "error sending data")
	}
	spew.Dump(dd)
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
		return int64(hIndex)
	}
	return HashNotFound
}

// BytesReader reads a file by blocks with given block size and sends them
type BytesReader struct {
	ctx      context.Context
	receiver chan<- *core.Message
	inbox    <-chan *core.Message
	senderID uuid.UUID
	id       int
}

func NewBytesReader(ctx context.Context, inbox <-chan *core.Message, receiver chan<- *core.Message, tempId int) *BytesReader {
	return &BytesReader{
		ctx:      ctx,
		receiver: receiver,
		inbox:    inbox,
		id:       tempId,
	}
}

func (w *BytesReader) Start() error {
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msg("bytes reader closing, context done")
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.FIN:
				log.Debug().
					Msgf("%d bytes reader received FIN", w.id)
				return nil
			case core.RSQ:
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Msgf("reader id %d,  byte reader received message", w.id)
				f, err := os.Open(msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName)
				if err != nil {
					return errors.Wrapf(err, "unable to read (missing) file %s", msg.FileDesc.FileName)
				}
				r := io.ReaderAt(f)
				sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
				br := bufio.NewReader(sr)
				buf := make([]byte, msg.FileDesc.BlockSize)

				for seq := int64(0); ; seq++ {
					dd := lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)

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
					_, err = dd.Write(buf)
					if err != nil {
						return errors.Wrap(err, "error reading file")
					}
					nMsg := &core.Message{
						Flag:     core.WSQ,
						FileDesc: msg.FileDesc,
						DataDesc: dd,
					}
					log.Trace().
						Str("filename", msg.FileDesc.FileName).
						Int64("dd len", int64(dd.Len())).
						Int64("block size", int64(msg.FileDesc.BlockSize)).
						Int64("offset", msg.Offset).
						Int64("seq", seq).
						Int("reader id", w.id).
						Msg("sending pure data")
					err = sendWithTimeout(nMsg, w.receiver)
					if err != nil {
						return errors.Wrap(err, "error sending data")
					}
				}
			default:
				return errors.New("BytesReader unknown message")
			}
		}
	}
}

// HasReader reads a file and calculates a hashList using a given block size
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
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("hash reader closing, context done")
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.FIN:
				log.Trace().
					Msg("hash reader received FIN")
				return nil
			case core.HSH:
				err := core.AddChecksums(msg.FileDesc)
				if err != nil {
					return errors.Wrap(err, "error calculating initial hash array")
				}
			default:
				return errors.New("HashReader unknown message")
			}
		}
	}
}
