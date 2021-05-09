package workers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
				err := w.rollV3(msg)
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

// rollV3 takes a filedesc from a message and parses the sender file,
// compares it with the hashMap and sends instruction to build the file
// on the receiver side
// 3rd implementation of the rolling hash reader
func (w *RollReader) rollV3(msg *core.Message) error {

	// open the file
	srcFilePath := filepath.Join(msg.FileDesc.Prefix, msg.FileDesc.FileName)
	fmt.Printf("path: %s\n", srcFilePath)
	log.Trace().
		Msgf("roll reader %d - start reading: %s", w.myID, srcFilePath)
	f, err := os.Open(srcFilePath)
	if err != nil {
		return errors.Wrapf(err, "roll reader %d - unable to open file for reading: %s", w.myID, srcFilePath)
	}
	defer f.Close()

	// section reader for paralell reading, buffered for performance
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
	// this seems to be much faster with small files, but still faster event with big files
	br := bufio.NewReader(sr)

	// create a hash map for faster sum lookup
	w.hMap = make(map[uint32]int)
	for i, h := range msg.FileDesc.Weak {
		w.hMap[h] = i
	}

	// initialize the rolling hash by copying first block of data
	rh := core.Pour(msg.FileDesc.BlockSize)
	n, err := io.CopyN(rh, br, msg.FileDesc.BlockSize)
	if err != nil || n < msg.FileDesc.BlockSize {
		// this should not happen, we should check for file size in advance
		return errors.Wrapf(err, "roll reader - failed to read file: %s", msg.FileDesc.FileName)
	}

	// fill the buffer with new block of data
	buf := new(bytes.Buffer)
	n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize)
	if n == 0 {
		// io.EOF shoudl be fine, but 0 bytes is definitelly not
		return errors.Wrapf(err, "roll reader - failed to read file: %s", msg.FileDesc.FileName)
	}

	// sequence counter for file recreation
	var seq int64

	// fresh data descriptor
	dd := lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)

	for {
		rSum := rh.Sum32()

		// send the data if we have enough
		if dd.Len() > msg.FileDesc.BlockSize {
			// new message
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

			// send
			err = sendWithTimeout(dMsg, w.receiver)
			w.msgCnt++
			if err != nil {
				return errors.Wrap(err, "error sending data")
			}
			// next!
			seq++
			dd = lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)
		}

		if hIndex, ok := w.hMap[rSum]; ok {

			/*********
			 * MATCH *
			 *********/

			// write index info
			err := dd.WriteIndex(int64(hIndex))
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write index data description")
			}
			w.indexCnt++
			log.Trace().Msgf("===================MATCH================== seq: %d", seq)
			//spew.Dump(rh.GetWindow())
			//log.Trace().Msg("===================MATCH==================")

			// we need to load a new block of data, so reset the hash first
			rh.Reset()

			// check if we have full buffer
			if int64(buf.Len()) < msg.FileDesc.BlockSize {
				// fill the buffer to max
				n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize-int64(buf.Len()))
				if n == 0 {
					if err == io.EOF {
						// no more data, end
						break
					} else {
						return errors.Wrapf(err, "roll reader - unable to read file %s", msg.FileDesc.FileName)
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
			n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize)
			if n == 0 {
				if err == io.EOF {
					// no more data, if there is something in the rh window, we'll append it at the end
					break
				} else {
					return errors.Wrapf(err, "roll reader - failed to read file %s", msg.FileDesc.FileName)
				}
			}
			continue
		} else {

			/************
			 * NO MATCH *
			 ************/

			// make sure we do not have an empty buffer
			if buf.Len() == 0 {
				n, err = io.CopyN(buf, br, msg.FileDesc.BlockSize)
				if n == 0 {
					if err == io.EOF {
						// no more data
						break
					} else {
						return errors.Wrapf(err, "roll reader - failed to read file %s", msg.FileDesc.FileName)
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
			w.dataCnt++
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
			w.dataCnt++
		}
	}
	// if there is trailing data in the buffer, append it
	if buf.Len() > 0 {
		for _, b := range buf.Bytes() {
			err = dd.WriteByte(b)
			if err != nil {
				return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
			}
			w.dataCnt++
		}
	}

	// send the last data package and close the transfer
	err = dd.MarkAsLast()
	if err != nil {
		return errors.Wrap(err, "roll reader - failed to write byte into file descriptor")
	}
	dMsg := &core.Message{
		Flag:     core.WSQ,
		FileDesc: msg.FileDesc, // maybe strip the useless data
		DataDesc: dd,
	}

	err = sendWithTimeout(dMsg, w.receiver)
	w.msgCnt++
	if err != nil {
		return errors.Wrap(err, "error sending data")
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
				p := filepath.Join(msg.FileDesc.Prefix, msg.FileDesc.FileName)
				f, err := os.Open(p)
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
							dd.MarkAsLast()
							nMsg := &core.Message{
								Flag:     core.WSQ,
								FileDesc: msg.FileDesc,
								DataDesc: dd,
							}
							err = sendWithTimeout(nMsg, w.receiver)
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
