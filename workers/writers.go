package workers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
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
	srcFd      *lfs.FileDesc
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
		srcFd:    fd,
		inbox:    inbox,
		senderID: uuid,
		dataSeq:  make(map[int64]*lfs.DataDesc),
		rBuf:     make([]byte, fd.BlockSize),
		wBuf:     make([]byte, fd.BlockSize),
	}
}

func (w FileWriter) Start() error {
	dstDir := viper.GetString("destination")
	tmpF, err := ioutil.TempFile(dstDir, w.srcFd.RelPath+".*."+w.senderID.String())
	if err != nil {
		return errors.Wrap(err, "unable to create file")
	}
	log.Trace().
		Str("file name", tmpF.Name()).
		Msg("DIFF opening temporary file")
	//defer tmpF.Close()
	defer os.Remove(tmpF.Name())

	w.bw = bufio.NewWriter(io.Writer(tmpF))
	oldPath := dstDir + "/" + w.srcFd.FileName
	// open a reader as well if we have to reference alredy present blocks
	if w.srcFd.State == lfs.Diff {
		log.Trace().
			Str("file name", oldPath).
			Msg("DIFF opening destination file for reference")
		f, err := os.Open(oldPath)
		if err != nil {
			errors.Wrap(err, "unable to open file for writer reference")
		}
		r := io.ReaderAt(f)
		w.sr = io.NewSectionReader(r, 0, int64(w.srcFd.FileSize))
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
				// account for out of order delivery, albeit might be not possible?
				//fmt.Println("<================== witer received WSQ message")
				//spew.Dump(msg)
				seq := msg.DataDesc.Seq()
				log.Trace().
					//Str("filename", msg.FileDesc.FileName).
					Int64("seq", seq).
					Int64("pSeq", w.pSeq).
					Msg("msg received by file writer")
				w.dataSeq[seq] = msg.DataDesc
				if seq == w.pSeq {
					// if we hae data at the current sequence, call writer
					err = w.write()
					if err != nil {
						return errors.Wrap(err, "unable to compare files")
					}
				}
			case core.FIN:
				log.Trace().
					Str("orig name", w.srcFd.FileName).
					Str("temp file path", tmpF.Name()).
					Str("rename to", dstDir+"/"+w.srcFd.FileName).
					Msg("file writer received FIN, renaming")
				err = os.Rename(tmpF.Name(), dstDir+"/"+w.srcFd.FileName)

				if err != nil {
					return errors.Wrap(err, "unable to replace file")
				}
				if err = tmpF.Close(); err != nil {
					return errors.Wrap(err, "unable to close file")
				}
				return nil
			default:
				return errors.New("unknown message received")
			}
		}
	}
}

func (w *FileWriter) write() error {
	dd := w.dataSeq[w.pSeq]
	br := bytes.NewReader(dd.Bytes())

	for {
		header := new(lfs.Header)
		err := binary.Read(br, binary.BigEndian, header)
		if err != nil {
			if err == io.EOF {
				// end of transmission
				w.bw.Flush()
				break
			} else {
				// nah something bad hapenned
				return errors.Wrap(err, "error reading data header")
			}
		}
		switch lfs.Flag(header.Flag) {
		case lfs.Data:
			//func CopyN(dst Writer, src Reader, n int64) (written int64, err error)
			_, err := io.CopyN(w.bw, br, header.Len)
			if err != nil {
				return errors.Wrap(err, "file write failed")
			}
			w.bw.Flush()
		case lfs.Index:
			// indexes
			hIndex := make([]int64, header.Len)
			err = binary.Read(br, binary.BigEndian, hIndex)
			if err != nil {
				return errors.Wrap(err, "error reading data")
			}
			for _, v := range hIndex {
				w.sr.Seek(v*w.srcFd.BlockSize, io.SeekStart)
				_, err = io.CopyN(w.bw, w.br, w.srcFd.BlockSize)
				if err != nil {
					return errors.Wrap(err, "error writing referenced data")
				}
				w.bw.Flush()
			}
		default:
			return errors.Wrap(err, "invalid header")
		}
	}
	w.pSeq++
	return nil
}
