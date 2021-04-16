package workers

import (
	"bufio"
	"bytes"
	"context"
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
	inbox      <-chan *core.Message
	senderID   uuid.UUID
	dstFd      *lfs.FileDesc
	dataSeq    map[int64]*lfs.DataDesc
	pSeq       int64 //"pointer" to the last sequence writtn
	rBuf, wBuf []byte
	bw         *bufio.Writer
	br         *bufio.Reader
}

func NewFileWriter(ctx context.Context, uuid uuid.UUID, fd *lfs.FileDesc, inbox <-chan *core.Message) FileWriter {
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
			r := io.ReaderAt(f)
			sr := io.NewSectionReader(r, 0, int64(w.dstFd.FileSize))
			w.br = bufio.NewReader(sr)
		}
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
	header := new(lfs.Header)
	br := bytes.NewReader(dd.Bytes)
	return nil
}
