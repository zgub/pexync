package workers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type FileWriter struct {
	ctx        context.Context
	inbox      chan *core.Message
	senderID   uuid.UUID
	dstFd      *lfs.FileDesc
	dataSeq    map[int64]*lfs.DataDesc
	pSeq       int64 //"pointer" to the last sequence writtn
	rBuf, wBuf []byte
	bw         *bufio.Writer
	sr         *io.SectionReader
	br         *bufio.Reader
}

func NewFileWriter(ctx context.Context, uuid uuid.UUID, fd *lfs.FileDesc, inbox chan *core.Message) FileWriter {
	return FileWriter{
		ctx:      ctx,
		dstFd:    fd,
		inbox:    inbox,
		senderID: uuid,
		dataSeq:  make(map[int64]*lfs.DataDesc),
		rBuf:     make([]byte, fd.BlockSize),
		wBuf:     make([]byte, fd.BlockSize),
	}
}

func (w FileWriter) Start() error {
	path := viper.GetString("local_destination") + "/" + w.dstFd.RelPath + "." + w.senderID.String()
	tmpF, err := os.Create(path)
	if err != nil {
		return errors.Wrap(err, "unable to create file")
	}
	defer tmpF.Close()

	w.bw = bufio.NewWriter(io.Writer(tmpF))

	// open a reader as well if we have to reference alredy present blocks
	if w.dstFd.State == lfs.Diff {
		path = w.dstFd.Prefix + "/" + w.dstFd.FileName
		f, err := os.Open(path)
		if err != nil {
			errors.Wrap(err, "unable to open file for writer reference")
		}
		r := io.ReaderAt(f)
		w.sr = io.NewSectionReader(r, 0, int64(w.dstFd.FileSize))
		w.br = bufio.NewReader(w.sr)
		defer f.Close()
	}

	for {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("file writer closing, context done")
			return nil
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.WSQ: // read sequence
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Msgf("msg received by file writer")
				// account for out of order delivery, albeit might be not possible?
				seq := msg.DataDesc.Seq()
				w.dataSeq[seq] = msg.DataDesc
				if seq == w.pSeq {
					// if we hae data at the current sequence, call writer
					err = w.write()
					if err != nil {
						return errors.Wrap(err, "unable to compare files")
					}
				}
			case core.FIN:
				log.Debug().
					Msg("file writer received FIN")
				return nil
			default:
				return errors.New("unknown message received")
			}
		}
	}
}

func (w *FileWriter) write() error {
	dd := w.dataSeq[w.pSeq]

	for {
		header := new(lfs.Header)
		br := bytes.NewReader(dd.Bytes())
		err := binary.Read(br, binary.BigEndian, header)
		if err != nil {
			if err == io.EOF {
				// end of transmission
				break
			} else {
				// nah something bad hapenned
				return errors.Wrap(err, "error reading data header")
			}
		}

		if header.Flag {
			// true means data
			//func CopyN(dst Writer, src Reader, n int64) (written int64, err error)
			fmt.Println("copying data")
			_, err = io.CopyN(w.bw, br, header.Len)
			if err != nil {
				return errors.Wrap(err, "file write failed")
			}
		} else {
			// indexes
			fmt.Println("copying indexes")
			hIndex := make([]int64, header.Len)
			err = binary.Read(br, binary.BigEndian, hIndex)
			if err != nil {
				return errors.Wrap(err, "error reading data")
			}
			for _, v := range hIndex {
				fmt.Printf("index value: %d\n", v)
				//spew.Dump(w.dstFd)
				//spew.Dump(w)
				w.sr.Seek(v*w.dstFd.BlockSize, io.SeekStart)
				_, err = io.CopyN(w.bw, w.br, w.dstFd.BlockSize)
				if err != nil {
					return errors.Wrap(err, "error writing referenced data")
				}
			}

		}
	}

	return nil
}
