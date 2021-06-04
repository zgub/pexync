package workers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

var (
	rrID, brID, frID int
)

// RollReader reads a file, compares the data witha hash table and send either data or indexes
type RollReader struct {
	myID                      int
	indexCnt, dataCnt, msgCnt int64 // counters
	ctx                       context.Context
	receiver                  chan<- *core.Message
	inbox                     <-chan *core.Message
	senderID                  uuid.UUID
	hMap                      map[uint32]int
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

func (rrw *RollReader) Start() error {

	for {
		// wait for file (or a section)
		select {
		case <-rrw.ctx.Done():
			log.Debug().
				Msgf("roll reader %d - closing, context done", rrw.myID)
			return nil
		case msg := <-rrw.inbox:
			switch msg.GetFlag() {
			case core.RSQ: // read sequence
				log.Debug().
					Str("filename", msg.GetFileDesc().FileName).
					Msgf("roll reader %d - file received", rrw.myID)
				err := rrw.rollV3(msg)
				if err != nil {
					return errors.Wrap(err, "roll hash reader failed")
				}
				log.Debug().
					Str("file name", msg.GetFileDesc().FileName).
					Int64("indexes", rrw.indexCnt).
					Int64("data", rrw.dataCnt).
					Int64("messages", rrw.msgCnt).
					Msgf("roll reader %d - stats", rrw.myID)
				// sigh
				/*
					if msg.FileLock != nil {
						log.Trace().
							Str("filename", msg.GetFileDesc().FileName).
							Msgf("XXXXXXXXXXXXXX ROLL READER %d UNLOCK XXXXXXXXXXXXX", rrw.myID)
						msg.FileLock.Unlock()
					}
				*/
			case core.FIN:
				log.Trace().
					Msgf("roll reader %d - received FIN", rrw.myID)
				return nil
			default:
				s := fmt.Sprintf("roll reader %d - unknown message received", rrw.myID)
				return errors.New(s)
			}
		}
	}
}

// rollV3 takes a filedesc from a message and parses the sender file,
// compares it with the hashMap and sends instruction to build the file
// on the receiver side
// 3rd implementation of the rolling hash reader
func (rrw *RollReader) rollV3(msg *core.Message) error {

	// sanity check
	streams := msg.GetStreamCount()
	if streams == 0 {
		return errors.New("zero data streams count")
	}

	// open the file
	srcFilePath := filepath.Join(msg.GetFileDesc().Prefix, msg.GetFileDesc().FileName)
	log.Trace().
		Msgf("roll reader %d - start reading: %s", rrw.myID, srcFilePath)
	f, err := os.Open(srcFilePath)
	if err != nil {
		return errors.Wrapf(err, "roll reader %d - unable to open file for reading: %s", rrw.myID, srcFilePath)
	}
	defer f.Close()

	// section reader for paralell reading, buffered for performance
	r := io.ReaderAt(f)
	offset := msg.GetOffset()
	limit := msg.GetLimit()
	sr := io.NewSectionReader(r, offset, limit)
	// this seems to be much faster with small files, but still faster event with big files
	br := bufio.NewReader(sr)

	// create a hash map for faster sum lookup
	rrw.hMap = make(map[uint32]int)
	for i, h := range msg.GetFileDesc().Weak {
		rrw.hMap[h] = i
	}

	// initialize the rolling hash by copying first block of data
	rh := core.Pour(msg.GetFileDesc().BlockSize)
	n, err := io.CopyN(rh, br, msg.GetFileDesc().BlockSize)
	if err != nil || n < msg.GetFileDesc().BlockSize {
		// this should not happen, we should check for file size in advance
		return errors.Wrapf(err, "roll reader - failed to read file: %s", msg.GetFileDesc().FileName)
	}

	// fill the buffer with new block of data
	buf := new(bytes.Buffer)
	n, err = io.CopyN(buf, br, msg.GetFileDesc().BlockSize)
	if n == 0 {
		// io.EOF shoudl be fine, but 0 bytes is definitelly not
		return errors.Wrapf(err, "roll reader - failed to read file: %s", msg.GetFileDesc().FileName)
	}

	// sequence counter for file recreation
	var seq int64

	// fresh data descriptor
	dd := lfs.NewDataDesc(msg.GetFileDesc().Idx, msg.GetOffset(), seq, streams)

	for {
		rSum := rh.Sum32()

		// send the data if we have enough
		if dd.Len() > msg.GetFileDesc().BlockSize {
			// new message
			dMsg := core.NewDataWSQ(dd, msg.GetFileDesc())

			/*
				log.Trace().
					Str("filename", msg.GetFileDesc().FileName).
					Int64("datadesc len", int64(dd.Len())).
					Int64("block size", msg.GetFileDesc().BlockSize).
					Int64("seq", dd.Seq()).
					Msgf("roll reader %d - sending data", rrw.myID)
			*/

			// send
			err = sendWithTimeout(dMsg, rrw.receiver)
			rrw.msgCnt++
			if err != nil {
				return errors.Wrap(err, "error sending data")
			}
			// next!
			seq++
			dd = lfs.NewDataDesc(msg.GetFileDesc().Idx, msg.GetOffset(), seq, streams)
		}

		if hIndex, ok := rrw.hMap[rSum]; ok {

			/*********
			 * MATCH *
			 *********/

			// write index info
			err := dd.WriteIndex(int64(hIndex))
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write index data description")
			}
			rrw.indexCnt++
			//log.Trace().Msgf("roll reader - BLOCK MATCH seq: %d", seq)

			// we need to load a new block of data, so reset the hash first
			rh.Reset()

			// check if we have full buffer
			if int64(buf.Len()) < msg.GetFileDesc().BlockSize {
				// fill the buffer to max
				n, err = io.CopyN(buf, br, msg.GetFileDesc().BlockSize-int64(buf.Len()))
				if n == 0 {
					if err == io.EOF {
						// no more data, end
						break
					} else {
						return errors.Wrapf(err, "roll reader - unable to read file %s", msg.GetFileDesc().FileName)
					}
				}
			}

			// re-initialize the rolling hash window
			m, err := rh.Write(buf.Bytes())
			if m == 0 {
				if err == io.EOF {
					break
				} else {
					return errors.Wrap(err, "roll reader - error initializing the roll hash")
				}
			}

			// load new block of data to the buffer
			buf.Reset()
			n, err = io.CopyN(buf, br, msg.GetFileDesc().BlockSize)
			if n == 0 {
				if err == io.EOF {
					// no more data, if there is something in the rh window, we'll append it at the end
					break
				} else {
					return errors.Wrapf(err, "roll reader - failed to read file %s", msg.GetFileDesc().FileName)
				}
			}
			continue
		} else {

			/************
			 * NO MATCH *
			 ************/

			// make sure we do not have an empty buffer
			if buf.Len() == 0 {
				n, err = io.CopyN(buf, br, msg.GetFileDesc().BlockSize)
				if n == 0 {
					if err == io.EOF {
						// no more data
						break
					} else {
						return errors.Wrapf(err, "roll reader - failed to read file %s", msg.GetFileDesc().FileName)
					}
				}
			}
			// read a byte from the buffer
			nb, err := buf.ReadByte()
			if err != nil {
				return errors.Wrapf(err, "roll reader - failed to read byte from the buffer")
			}

			// push the new byte into the roll hash emitting the oldest one
			ob := rh.Roll(nb)

			// write the not matching old byte to the file descriptor
			err = dd.WriteByte(ob)
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
			}
			rrw.dataCnt++
		}

	}

	rhWindow := rh.GetWindow()
	// if there is trailing data in the hash, append it
	if len(rhWindow) > 0 {
		for _, b := range rhWindow {
			err = dd.WriteByte(b)
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
			}
			rrw.dataCnt++
		}
	}
	// if there is trailing data in the buffer, append it
	if buf.Len() > 0 {
		for _, b := range buf.Bytes() {
			err = dd.WriteByte(b)
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
			}
			rrw.dataCnt++
		}
	}

	// send the last data package and close the transfer
	err = dd.MarkAsLast()
	if err != nil {
		return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
	}
	dMsg := core.NewDataWSQ(dd, msg.GetFileDesc())

	err = sendWithTimeout(dMsg, rrw.receiver)
	if err != nil {
		return errors.Wrap(err, "error sending data")
	}
	rrw.msgCnt++

	return nil
}

// BytesReader reads a file by blocks with given block size and sends them
type BytesReader struct {
	myID     int
	ctx      context.Context
	receiver chan<- *core.Message
	inbox    <-chan *core.Message
	senderID uuid.UUID
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

func (brw *BytesReader) Start() error {
	log.Debug().
		Msgf("bytes reader - %d starting", brw.myID)

	for {
		select {
		case <-brw.ctx.Done():
			log.Debug().
				Msgf("bytes reader %d - closing, context done", brw.myID)
			return nil
		case msg := <-brw.inbox:
			switch msg.GetFlag() {
			case core.FIN:
				log.Debug().
					Msgf("bytes reader %d - received FIN", brw.myID)
				return nil
			case core.RSQ:

				// sanity check
				streams := msg.GetStreamCount()
				if streams == 0 {
					panic("bytes reader: zero stream count")
				}

				log.Trace().
					Str("filename", msg.GetFileDesc().FileName).
					Msgf("bytes reader %d - message received", brw.myID)
				p := filepath.Join(msg.GetFileDesc().Prefix, msg.GetFileDesc().FileName)
				f, err := os.Open(p)
				if err != nil {
					return errors.Wrapf(err, "unable to read (missing) file %s", msg.GetFileDesc().FileName)
				}
				r := io.ReaderAt(f)
				sr := io.NewSectionReader(r, msg.GetOffset(), msg.GetLimit())
				br := bufio.NewReader(sr)
				buf := make([]byte, msg.GetFileDesc().BlockSize)

				for seq := int64(0); ; seq++ {
					dd := lfs.NewDataDesc(msg.GetFileDesc().Idx, msg.GetOffset(), seq, streams)

					n, err := io.ReadFull(br, buf)
					if n == 0 {
						if err == nil {
							return errors.New("read 0 bytes")
						} else if err != io.EOF {
							return errors.Wrap(err, "error reading file")
						}
						if err == io.EOF {
							// end of transmission
							dd.MarkAsLast()
							nMsg := core.NewDataWSQ(dd, msg.GetFileDesc())
							err = sendWithTimeout(nMsg, brw.receiver)
							if err != nil {
								return errors.Wrap(err, "error sending data")
							}
							break
						}
					}
					buf = buf[:n]
					_, err = dd.Write(buf)
					if err != nil {
						return errors.Wrap(err, "error reading file")
					}
					nMsg := core.NewDataWSQ(dd, msg.GetFileDesc())
					/*
						log.Trace().
							Str("filename", msg.GetFileDesc().FileName).
							Int64("dd len", int64(dd.Len())).
							Int64("block size", int64(msg.GetFileDesc().BlockSize)).
							Int64("offset", msg.GetOffset()).
							Int64("seq", seq).
							Msgf("bytes reader %d - sending pure data", brw.myID)
					*/
					err = sendWithTimeout(nMsg, brw.receiver)
					if err != nil {
						return errors.Wrap(err, "error sending data")
					}
					/*
						log.Trace().
							Msgf("bytes reader - %d data sent", brw.myID)
					*/

				}
				/*
					if msg.FileLock != nil {
						log.Trace().
							Str("filename", msg.GetFileDesc().FileName).
							Msg("XXXXXXXXXXXXXX BYTES READER UNLOCK XXXXXXXXXXXXX")
						msg.FileLock.Unlock()
					}
				*/
			default:
				return errors.New("BytesReader unknown message")
			}
		case <-time.After(3 * time.Second):
			fmt.Printf("bytes reader %d - timeout\n", brw.myID)
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

func (hrw *HashReader) Start() error {
	for {
		select {
		case <-hrw.ctx.Done():
			log.Debug().
				Msg("hash reader - closing, context done")
			return nil
		case msg := <-hrw.inbox:
			switch msg.GetFlag() {
			case core.FIN:
				log.Trace().
					Msg("hash reader - received FIN")
				return nil
			case core.HSH:
				fd := msg.GetFileDesc()
				if fd.IsDir == false {
					err := core.AddChecksums(msg.GetFileDesc())
					if err != nil {
						return errors.Wrap(err, "error calculating initial hash array")
					}
				}
			default:
				return errors.New("hash reader - unknown message")
			}
		}
	}
}

type FileReader struct {
	myID                      int
	indexCnt, dataCnt, msgCnt int64
	ctx                       context.Context
	receiver                  chan<- *core.Message
	inbox                     <-chan *core.Message
	senderID                  uuid.UUID
	hMap                      map[uint32]int
}

// NewFileReader returns a new genreal file reader
func NeFileReader(ctx context.Context, inbox <chan *core.Message, recevier chan<- *core.Message) *FileReader {
	frID++
	return &FileReader{
		ctx: ctx,
		receiver: recevier,
		inbox: inbox,
		myID: frID,
	}
}

// Start reads a file either block by block (if new) or by using a rollReader
func (frw *FileReader) Start() error {
	log.Debug().
		Msgf("file reader - %d starting")
	return nil
}
