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

const (
	HashNotFound int64 = -1
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
	buf                       *bytes.Buffer // roll byte accumulator
	p                         int
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

func (w *RollReader) rollV1(msg *core.Message) error {
	log.Trace().
		Msgf("roll reader %d - starting", w.myID)

	path := msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName
	// this could be possibly optimized in order to save file descriptors
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "unable to open file for hash comparison")
	}
	defer f.Close()

	// boring
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
	br := bufio.NewReader(sr)
	buf := make([]byte, msg.FileDesc.BlockSize)

	// create a hash map for faster lookup
	w.hMap = make(map[uint32]int)
	for i, h := range msg.FileDesc.Weak {
		w.hMap[h] = i
	}

	// read the first block of data
	n, err := io.ReadFull(br, buf)
	if n == 0 {
		if err == nil {
			return errors.Wrapf(err, "%s - hash comparator receiver 0 bytes while readind the file", msg.FileDesc.FileName)
		} else if err != io.EOF {
			return errors.Wrapf(err, "%s error reading file while comparing hashes", msg.FileDesc.FileName)
		}
		if err == io.EOF {
			log.Trace().
				Msg("roll reader %d - EOF - while reading first buffer")
			return nil
		}
	}
	buf = buf[:n]

	// initialize the roll buffer by writting the first block
	rh := core.Pour(msg.FileDesc.BlockSize)
	_, err = rh.Write(buf)
	if err != nil {
		return errors.Wrap(err, "error writing to roll window")
	}

	// initialize sequence counter
	seq := int64(0)

	// create data description struct
	dd := lfs.NewDataDesc(msg.FileDesc.Idx, msg.Offset, seq)

	// search for the block sum in the hash map
	hIndex := w.lookup(rh.Sum32())
	if err != nil {
		return errors.Wrap(err, "hash lookup failure")
	}

	// check if already the first block matches
	if hIndex != HashNotFound {
		// first block already matches
		w.indexCnt++
		fmt.Println("*** first hash match ***")
		err = dd.WriteIndex(hIndex)
		if err != nil {
			return errors.Wrap(err, "roll hash calculation failed")
		}
		dd.Print("fisr hash matched")
	}
	// store the accumulator buf in case it did not match
	//_, err = w.accBuff.Write(buf)

	log.Trace().
		Str("filename", msg.FileDesc.FileName).
		Int64("block size", msg.FileDesc.BlockSize).
		Bool("first block skipped?", hIndex != HashNotFound).
		Msgf("roll reader %d", w.myID)
	// now continue through the rest of the file secion

	/*************************
	 * rolling hash cycle    *
	 *************************/
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
					Msgf("roll reader %d - EOF", w.myID)
				break
			}
		}
		// not sure if necessary, but ...
		buf = buf[:n]
		log.Trace().
			Str("block data", string(buf)).
			Int("new block length", len(buf)).
			Int("bytes read", n).
			Int("dd len", int(dd.Len())).
			Msgf("roll reader %d - reading new block", w.myID)

		// check if the previous block was a hash match
		if hIndex != HashNotFound {
			// if it was

			// cleanup first
			rh.Reset()
			//w.accBuff.Reset()

			// write the new data to roll hash buffer
			_, err := rh.Write(buf)
			if err != nil {
				return errors.Wrap(err, "error writing to roll window")
			}

			// calculate a new hash from using the new block
			sum := rh.Sum32()
			// lookup the new hash again
			hIndex = w.lookup(sum)
			if hIndex != HashNotFound {
				w.indexCnt++
				// again matching block, next!
				err = dd.WriteIndex(hIndex)
				if err != nil {
					return errors.Wrap(err, "roll hash calculation failed")
				}
				// another match, start the cycle again
				continue
			}
			// the previous one was a match
			// but this one is not, we need to continue
		}

		fmt.Println("*** no luck this time ***")
		dd.Print("second hash no match")
		// ok, no luck last time, let's rock and
		for _, b := range buf {
			fmt.Print(".")
			// add a byte
			rh.Roll(b)

			// lookup in the remote file hash list
			hIndex = w.lookup(rh.Sum32())
			if hIndex != HashNotFound {
				// another match
				err = dd.WriteIndex(hIndex)
				if err != nil {
					return errors.Wrap(err, "roll hash calculation failed")
				}
				fmt.Printf("\n+ %d\n", w.indexCnt)
				dd.Print("hash match")
				w.indexCnt++
				fmt.Println("")
				continue
			}
			// no luck this time
			// first add the oldest byte to the send buffer
			//dd.WriteByte(w.pop())
			// check the sendBuf size and send it eventually
			if dd.Len() > msg.FileDesc.BlockSize {
				// append is not thread safe!
				nMsg := &core.Message{
					Flag:     core.WSQ,
					FileDesc: msg.FileDesc,
					DataDesc: dd,
				}
				fmt.Println("")
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Int64("datadesc len", int64(dd.Len())).
					Int64("block size", msg.FileDesc.BlockSize).
					Msgf("roll reader %d - sending data", w.myID)
				dd.Print("sending and creating new dd")
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
			//w.push(b)
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
		Msgf("roll reader %d - sending remaining data", w.myID)
	err = sendWithTimeout(nMsg, w.receiver)
	w.msgCnt++
	if err != nil {
		return errors.Wrap(err, "error sending data")
	}
	dd.Print("remaining data")
	return nil
}

func (w *RollReader) rollV2(msg *core.Message) error {
	log.Trace().
		Msgf("roll reader %d - starting", w.myID)

	// open the file to roll
	srcFilePath := msg.FileDesc.Prefix + "/" + msg.FileDesc.FileName
	f, err := os.Open(srcFilePath)
	if err != nil {
		return errors.Wrapf(err, "roll reader %d - unable to open file for reading: %s", w.myID, srcFilePath)
	}
	defer f.Close()

	// now the usuall stuff
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, msg.Offset, msg.Limit)
	br := bufio.NewReader(sr)
	// we need exact size for initialization
	initBuf := make([]byte, msg.FileDesc.BlockSize)
	n, err := io.ReadFull(br, initBuf)
	if n == 0 {
		if err == io.ErrUnexpectedEOF {
			// this should not happen!!!
			return errors.Wrapf(err, "roll reader %d - loaded less bytes than blocksize during hash initialization", w.myID)
		}
		return errors.Wrapf(err, "roll reader %d - unable to read file", w.myID)
	}

	// create a hash map for faster lookup
	w.hMap = make(map[uint32]int)
	for i, h := range msg.FileDesc.Weak {
		w.hMap[h] = i
	}

	// read the firts block to initialize the roll window
	m, err := io.Copy(w.buf, br)
	if m == 0 {
		if err == nil {
			return errors.Wrapf(err, "roll reader - 0 bytes read from file: %s", msg.FileDesc.FileName)
		}
		if err == io.EOF {
			log.Warn().
				Msgf("roll reader %d - got EOF at first block")
			return nil
		}
	}

	//buf = buf[:n]

	// initialize the rolling Adler hash
	rh := core.Pour(msg.FileDesc.BlockSize)
	_, err = rh.Write(w.buf.Bytes())
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
				Msgf("roll reader %d - sending data", w.myID)

			err = sendWithTimeout(dMsg, w.receiver)
			w.msgCnt++
			if err != nil {
				return errors.Wrap(err, "error sending data")
			}
			seq++

		}
	}

	//return nil
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
