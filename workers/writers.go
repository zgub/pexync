package workers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
	ctx       context.Context
	inbox     chan *core.Message
	senderID  uuid.UUID
	srcFd     *lfs.FileDesc
	seqBuffer map[int64]*lfs.DataDesc
	pSeq      int64 // last sequence written
	bw        *bufio.Writer
	sr        *io.SectionReader
}

func NewFileWriter(ctx context.Context, uuid uuid.UUID, fd *lfs.FileDesc, inbox chan *core.Message) FileWriter {
	return FileWriter{
		ctx:       ctx,
		srcFd:     fd,
		inbox:     inbox,
		senderID:  uuid,
		seqBuffer: make(map[int64]*lfs.DataDesc),
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
		Msg("file writer - DIFF opening temporary file")

	w.bw = bufio.NewWriter(io.Writer(tmpF))
	oldPath := dstDir + "/" + w.srcFd.FileName
	// open a reader as well if we have to reference alredy present blocks
	if w.srcFd.State == lfs.Diff {
		log.Trace().
			Str("file name", oldPath).
			Msg("file writer - DIFF opening destination file for reference")
		f, err := os.Open(oldPath)
		if err != nil {
			errors.Wrap(err, "unable to open file for writer reference")
		}
		r := io.ReaderAt(f)
		w.sr = io.NewSectionReader(r, 0, int64(w.srcFd.FileSize))
		defer f.Close()
	}

Loop:
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msg("file writer - closing, context done")
			break Loop
		case msg := <-w.inbox:
			if msg.Flag != core.WSQ {
				return errors.New("file writer - invalide message type")
			}
			seq := msg.DataDesc.Seq()
			log.Trace().
				//Str("filename", msg.FileDesc.FileName).
				Int64("seq", seq).
				Int64("pSeq", w.pSeq).
				Msg("file writer -  msg received")
			//w.dataSeq[seq] = msg.DataDesc
			if seq == w.pSeq {
				// if we hae data at the current sequence, call writer
				//spew.Dump(msg)
				err = w.writeToFile(msg.DataDesc)
				if err != nil {
					if err == lfs.ErrEOF {
						break Loop
					}
					return errors.Wrap(err, "unable to write file")
				}
				// increase the expected sequence number
				w.pSeq++
				// letch chekc whether we have some other data to write
				haveCached := func() bool {
					_, ok := w.seqBuffer[w.pSeq]
					return ok
				}
				for haveCached() {
					err = w.writeToFile(w.seqBuffer[w.pSeq])
					if err != nil {
						if err == lfs.ErrEOF {
							break Loop
						}
						return errors.Wrap(err, "unable to write file")
					}
					// release memory
					delete(w.seqBuffer, w.pSeq)
					// increase the expected sequence number again
					w.pSeq++
				}
			} else {
				// out of order delivery, store it - there is only one writer goroutine per file, so the shouldn't be multiple accesses to this map
				w.seqBuffer[msg.DataDesc.Seq()] = msg.DataDesc
				log.Warn().
					Int64("got", seq).
					Int64("expecting", w.pSeq).
					Msg("out of order - caching")
			}

		}
	}

	log.Trace().
		Str("orig name", w.srcFd.FileName).
		Str("temp file path", tmpF.Name()).
		Str("rename to", dstDir+"/"+w.srcFd.FileName).
		Msg("file writer - finished, renaming")

	// first close
	if err = tmpF.Close(); err != nil {
		return errors.Wrap(err, "unable to close file")
	}

	// now rename
	fmt.Printf("renaming: %s to %s\n", tmpF.Name(), dstDir+"/"+w.srcFd.FileName)
	err = os.Rename(tmpF.Name(), dstDir+"/"+w.srcFd.FileName)
	if err != nil {
		return errors.Wrap(err, "unable to replace file")
	}
	return nil
}

// writeToFile reades a data stream and reconstructs a file based on headders
func (w *FileWriter) writeToFile(dd *lfs.DataDesc) error {
	fmt.Printf("==========> write() - sequence %d, write() start \n", w.pSeq)
	br := bytes.NewReader(dd.Bytes())

	for z := 0; ; z++ {
		header := new(lfs.Header)
		err := binary.Read(br, binary.BigEndian, header)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("========> c: %d writing sequence %d, EOF \n", z, w.pSeq)
				// end of transmission
				w.bw.Flush()
				break
			} else {
				// nah something bad hapenned
				fmt.Printf("========> c: %d writing sequence %d, error \n", z, w.pSeq)
				return errors.Wrap(err, "error reading data header")
			}
		}
		switch lfs.Flag(header.Flag) {
		case lfs.Data:
			//func CopyN(dst Writer, src Reader, n int64) (written int64, err error)
			fmt.Printf("========> c: %d writing sequence %d, writing data \n", z, w.pSeq)
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
				fmt.Printf("========> c: %d writing sequence %d, writting index %d \n", z, w.pSeq, v)
				n, err := w.sr.Seek(v*w.srcFd.BlockSize, io.SeekStart)
				if err != nil {
					return errors.Wrap(err, "failed to seek")
				}
				log.Trace().
					Int64("seek", n).
					Int64("location", v*w.srcFd.BlockSize).
					Msg("seek")
				_, err = io.CopyN(w.bw, w.sr, w.srcFd.BlockSize)
				if err != nil {
					return errors.Wrap(err, "error writing referenced data")
				}
				w.bw.Flush()
			}
		case lfs.End:
			return lfs.ErrEOF
		default:
			fmt.Printf("========> c: %d writing sequence %d, invalid header \n", z, w.pSeq)
			return errors.New("file writer - invalid header")
		}
	}
	return nil
}
