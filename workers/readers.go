package workers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

var (
	rrID, brID int
)

// RollReader reads a file, compares the data witha hash table and send either data or indexes
type RollReader struct {
	ctx                       context.Context
	receiver                  chan<- *core.Message
	inbox                     <-chan *core.Message
	senderID                  uuid.UUID
	hMap                      map[uint32]int
	indexCnt, dataCnt, msgCnt int64
	myID                      int
}

func NewRollReader(ctx context.Context, inbox <-chan *core.Message, receiver chan<- *core.Message) *RollReader {
	rrID++
	return &RollReader{
		ctx:      ctx,
		inbox:    inbox,
		receiver: receiver,
		myID:     rrID,
	}
}

func (w *RollReader) Start() error {

	for {
		// wait for file (or a section)
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msgf("roll reader %d - closing, context done", w.myID)
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.RSQ: // read sequence
				log.Debug().
					Str("filename", msg.FileDesc.FileName).
					Msgf("roll reader %d - file received", w.myID)
				err := w.rollV2(msg)
				if err != nil {
					return errors.Wrap(err, "roll hash reader failed")
				}
				log.Debug().
					Str("file name", msg.FileDesc.FileName).
					Int64("indexes", w.indexCnt).
					Int64("data", w.dataCnt).
					Int64("messages", w.msgCnt).
					Msgf("roll reader %d - stats", w.myID)
			case core.FIN:
				log.Trace().
					Msgf("roll reader %d - received FIN", w.myID)
				return nil
			default:
				s := fmt.Sprintf("roll reader %d - unknown message received", w.myID)
				return errors.New(s)
			}
		}
	}
}

func (w *RollReader) rollV2(msg *core.Message) error {

	// open the file to roll
	srcFilePath := msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName
	log.Trace().
		Msgf("roll reader %d - start reading: %s", w.myID, srcFilePath)
	f, err := os.Open(srcFilePath)
	if err != nil {
		return errors.Wrapf(err, "roll reader %d - unable to open file for reading: %s", w.myID, srcFilePath)
	}
	defer f.Close()

	// now the usuall stuff
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
	// this seems to be much faster with small files, but still faster event with big files
	br := bufio.NewReader(sr)
	// we need exact size for initialization
	buf := new(bytes.Buffer)

	// read the initial block
	n, err := io.CopyN(buf, br, msg.FileDesc.BlockSize)
	if n == 0 {
		if err == io.EOF {
			// this should not happen!!!
			return errors.Wrapf(err, "roll reader %d - empty file", w.myID)
		}
		return errors.Wrapf(err, "roll reader %d - unable to read file", w.myID)
	}

	// create a hash map for faster lookup
	w.hMap = make(map[uint32]int)
	for i, h := range msg.FileDesc.Weak {
		w.hMap[h] = i
	}

	log.Trace().
		Msgf("roll reader %d - initializing rolling hash", w.myID)
	// initialize the rolling Adler hash
	rh := core.Pour(msg.FileDesc.BlockSize)
	_, err = rh.Write(buf.Bytes())
	if err != nil {
		return errors.Wrapf(err, "roll reader %d, unable to initialize the roll hash window", w.myID)
	}

	// sequence counter
	var seq int64

	// data descriptor
	dd := lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)

	// let's roll
	for {
		// check the dd data size
		if dd.Len() > msg.FileDesc.BlockSize {
			dMsg := &core.Message{
				Flag:     core.WSQ,
				FileDesc: msg.FileDesc, // maybe strip the useless data
				DataDesc: dd,
			}

			log.Trace().
				Str("filename", msg.FileDesc.FileName).
				Int64("datadesc len", int64(dd.Len())).
				Int64("block size", msg.FileDesc.BlockSize).
				Int64("seq", dd.Seq()).
				Msgf("roll reader %d - sending data", w.myID)

			err = sendWithTimeout(dMsg, w.receiver)
			w.msgCnt++
			if err != nil {
				return errors.Wrap(err, "error sending data")
			}
			seq++
			dd = lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)
		}

		// calculate Adler32 digest from the data in rh window
		sum := rh.Sum32()

		if hIndex, ok := w.hMap[sum]; ok {
			// MATCH !!!

			err := dd.WriteIndex(int64(hIndex))
			if err != nil {
				return errors.Wrapf(err, "roll reader %d - failed to write index data description", w.myID)
			}

			// we need to load more data for the next round
			// just make sure that buffer contains only blocksize of data
			// it might hold some old data from a byte by byte roll run
			if int64(buf.Len()) < msg.FileDesc.BlockSize {
				// reset the buffer this time, to avoid appending
				n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize-int64(buf.Len()))
			} else {
				buf.Reset()
				n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize)
			}
			if n == 0 {
				if err == io.EOF {
					break
				} else {
					errors.Wrap(err, "roll reader %d - failed to read file")
				}
			}

			// write appends, so reset first, the write nw data
			rh.Reset()
			_, err = rh.Write(buf.Bytes())
			if err != nil {
				return errors.Wrapf(err, "roll reader %d, unable to calculate rolling hash", w.myID)
			}
		} else {
			// DOES NOT MATCH
			// we have old buf in rh window
			// we have new data in the buf
			nb, err := buf.ReadByte()
			if err == io.EOF {
				// empty buffer
				// load a new block of data
				// first clear the buffer
				buf.Reset()
				n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize)
				if n == 0 {
					if err == io.EOF {
						break
					} else {
						errors.Wrap(err, "roll reader %d - failed to read file")
					}
				}

				// reset the rhash and write new data
				rh.Reset()
				_, err = rh.Write(buf.Bytes())
				if err != nil {
					return errors.Wrapf(err, "roll reader %d, unable to calculate rolling hash", w.myID)
				}
				// next!!!
				continue
			}
			ob := rh.Roll(nb)
			err = dd.WriteByte(ob)
			if err != nil {
				return errors.Wrapf(err, "roll reader %d - failed to write bytes into data description", w.myID)
			}
		}
	}

	if dd.Len() > 0 {
		dMsg := &core.Message{
			Flag:     core.WSQ,
			FileDesc: msg.FileDesc, // maybe strip the useless data
			DataDesc: dd,
		}

		log.Trace().
			Str("filename", msg.FileDesc.FileName).
			Int64("datadesc len", int64(dd.Len())).
			Int64("block size", msg.FileDesc.BlockSize).
			Msgf("roll reader %d - sending remaining data", w.myID)

		err = sendWithTimeout(dMsg, w.receiver)
		w.msgCnt++
		if err != nil {
			return errors.Wrap(err, "error sending data")
		}
		seq++
		dd = lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)
	}

	return nil
}

// BytesReader reads a file by blocks with given block size and sends them
type BytesReader struct {
	ctx      context.Context
	receiver chan<- *core.Message
	inbox    <-chan *core.Message
	senderID uuid.UUID
	myID     int
}

func NewBytesReader(ctx context.Context, inbox <-chan *core.Message, receiver chan<- *core.Message) *BytesReader {
	brID++
	return &BytesReader{
		ctx:      ctx,
		receiver: receiver,
		inbox:    inbox,
		myID:     brID,
	}
}

func (w *BytesReader) Start() error {
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msgf("bytes reader %d - closing, context done", w.myID)
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.FIN:
				log.Debug().
					Msgf("bytes reader %d - received FIN", w.myID)
				return nil
			case core.RSQ:
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Msgf("bytes reader %d - message received", w.myID)
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
							// end of transmission
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
						Msgf("bytes reader %d - sending pure data", w.myID)
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
			log.Debug().
				Msg("hash reader - closing, context done")
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.FIN:
				log.Trace().
					Msg("hash reader - received FIN")
				return nil
			case core.HSH:
				err := core.AddChecksums(msg.FileDesc)
				if err != nil {
					return errors.Wrap(err, "error calculating initial hash array")
				}
			default:
				return errors.New("hash reader - unknown message")
			}
		}
	}
}
