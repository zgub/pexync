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
	"path/filepath"
	"sort"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type tmpFile struct {
	f       *os.File
	w       *bufio.Writer
	seq     int64
	dataBuf map[int64]*lfs.DataDesc
	path    string
}

type FileWriter struct {
	ctx      context.Context
	inbox    chan *core.Message
	senderID uuid.UUID
	srcFd    *lfs.FileDesc
	rr       io.Reader // reference file reader
	fileMap  map[int64]*tmpFile
	streams  int64 // number of incomming streams
}

func NewFileWriter(ctx context.Context, uuid uuid.UUID, streams int64, fd *lfs.FileDesc, inbox chan *core.Message) FileWriter {
	return FileWriter{
		ctx:      ctx,
		streams:  streams,
		srcFd:    fd,
		inbox:    inbox,
		senderID: uuid,
		fileMap:  make(map[int64]*tmpFile),
	}
}

func (fw FileWriter) Start() error {
	var err error

	dstDir := viper.GetString("destination")
	dstPath := filepath.Join(dstDir, fw.srcFd.FileName)

	// open a reader as well if we have to reference alredy present blocks
	if fw.srcFd.State == lfs.Diff {
		// first rename the old file
		refName := dstPath + ".ref"
		err = os.Rename(dstPath, refName)
		log.Trace().
			Str("existing file name", dstPath).
			Str("renamed to", refName).
			Msg("file writer - DIFF opening destination file for reference")
		ref, err := os.Open(refName)
		if err != nil {
			errors.Wrap(err, "unable to open file for writer reference")
		}
		fw.rr = io.Reader(ref)
		defer ref.Close()
	}

Loop:
	for {
		select {
		case <-fw.ctx.Done():
			log.Debug().
				Msg("file writer - closing, context done")
			break Loop
		case msg := <-fw.inbox:
			switch msg.GetFlag() {
			case core.WSQ:
				// data sequence (ref index or byte date)

				seq := msg.GetDataDesc().Seq()
				offset := msg.GetDataDesc().Offset()
				log.Trace().
					Int64("offset", offset).
					Int64("seq", seq).
					Msg("file writer -  msg received")

				// check if we;re already oepend a temp file for the paralel stream
				if _, ok := fw.fileMap[offset]; !ok {
					err = fw.newTempFile(offset)
					if err != nil {
						return errors.Wrap(err, "file writer - failed opening temporary file")
					}
				}
				tmpF := fw.fileMap[offset]
				// we already are processing this stream
				// check the sequence
				if seq == tmpF.seq {
					err := fw.writeToFile(msg.GetDataDesc())
					if err != nil {
						if err == lfs.ErrEOF {
							// end of chink, close tmp file
							err = tmpF.f.Close()
							if err != nil {
								errors.Wrap(err, "unable to close file")
							}
							log.Debug().
								Str("file name", dstPath).
								Int64("offset chunk", offset).
								Msg("file writer - xxxxxxxxxxxxxxxxxxxxxx closing temporary file")
							continue
						}
						return errors.Wrap(err, "unable to write file")
					}
					// increase the sequence counter
					tmpF.seq++

					haveCached := func() bool {
						_, ok := tmpF.dataBuf[tmpF.seq]
						return ok
					}

					for haveCached() {
						err = fw.writeToFile(tmpF.dataBuf[tmpF.seq])
						if err != nil {
							if err == lfs.ErrEOF {
								err = tmpF.f.Close()
								if err != nil {
									errors.Wrap(err, "unable to close file")
								}
								log.Debug().
									Str("file name", dstPath).
									Int64("offset chunk", offset).
									Msg("file writer - cccccccccccccccccccccc closing temporary file")
								continue
							}
							return errors.Wrap(err, "unable to write file")
						}
						// release memory
						delete(tmpF.dataBuf, tmpF.seq)
						// increase the expected sequence number again
						tmpF.seq++
					}

				} else {
					tmpF.dataBuf[seq] = msg.GetDataDesc()
					log.Warn().
						Int64("got", seq).
						Int64("expecting", tmpF.seq).
						Msg("out of order - caching")
				}
			default:
				return errors.New("file writer - invalide message type")
			}
		}
	}

	log.Trace().
		Str("orig name", fw.srcFd.FileName).
		Str("merging to", dstPath).
		Msg("file writer - finished, rebuilding")

	// if there is only one, just rename
	if len(fw.fileMap) == 1 {
		err = os.Rename(fw.fileMap[0].f.Name(), dstPath)
		if err != nil {
			return errors.Wrap(err, "unable to replace file")
		}
	} else {
		fmt.Printf("++++++++ temp file map size %d\n", len(fw.fileMap))
		// large file, we need to reconstruct it from several tmp fil
		nf, err := os.Create(dstPath)
		if err != nil {
			return errors.Wrap(err, "unable to open file")
		}
		tmpOffsets := make([]int64, 0)
		fmt.Printf("tmpOffsets len %d fileMap len %d\n", len(tmpOffsets), len(fw.fileMap))
		for offset := range fw.fileMap {
			log.Debug().
				Int64("offset", offset).
				Msg("collecting")
			tmpOffsets = append(tmpOffsets, offset)
		}

		fmt.Printf("++++++++ tmpOffsets size %d\n%+v\n", len(tmpOffsets), tmpOffsets)

		// they shoudl add sort.Int64() but ... I know that int = int64 on most systems, but I don't like assumptions like that
		sort.Slice(tmpOffsets, func(i, j int) bool { return tmpOffsets[i] > tmpOffsets[j] })

		bw := bufio.NewWriter(io.Writer(nf))

		fmt.Printf("++++++++ tmpOffsets size %d\n%+v\n", len(tmpOffsets), tmpOffsets)

		for _, offset := range tmpOffsets {
			tf := fw.fileMap[offset]
			tf.f, err = os.Open(tf.path)
			if err != nil {
				return errors.Wrap(err, "failed to open temporary file")
			}
			br := bufio.NewReader(io.Reader(tf.f))
			fmt.Printf("merging offset %d into %s\n", offset, dstPath)
			n, err := io.Copy(bw, br)
			if err != nil {
				return errors.Wrap(err, "failed to reconstruct file")
			}
			log.Trace().
				Int64("file offset", offset).
				Int64("bytes written", n).
				Str("filename", tf.path).
				Msg("file writter - reconstructing")
		}
		fmt.Println("merge done")

	}
	return nil
}

// writeToFile reades a data stream and reconstructs a file based on headders
func (fw *FileWriter) writeToFile(dd *lfs.DataDesc) error {
	br := bytes.NewReader(dd.Bytes())
	offset := dd.Offset()
	f := fw.fileMap[offset].f
	w := fw.fileMap[offset].w

	for z := 0; ; z++ {
		header := new(lfs.Header)
		err := binary.Read(br, binary.BigEndian, header)
		if err != nil {
			if err == io.EOF {
				// end of transmission
				w.Flush()
				break
			} else {
				// nah something bad hapenned
				return errors.Wrap(err, "error reading data header")
			}
		}
		switch lfs.Flag(header.Flag) {
		case lfs.Data:
			//func CopyN(dst Writer, src Reader, n int64) (written int64, err error)
			_, err := io.CopyN(w, br, header.Len)
			if err != nil {
				return errors.Wrap(err, "file write failed")
			}
			w.Flush()
		case lfs.Index:
			// indexes
			hIndex := make([]int64, header.Len)
			err = binary.Read(br, binary.BigEndian, hIndex)
			if err != nil {
				return errors.Wrap(err, "error reading data")
			}
			for _, v := range hIndex {
				n, err := f.Seek(v*fw.srcFd.BlockSize, io.SeekStart)
				if err != nil {
					return errors.Wrap(err, "failed to seek")
				}
				log.Trace().
					Int64("seek", n).
					Int64("location", v*fw.srcFd.BlockSize).
					Msg("seek")
				_, err = io.CopyN(w, fw.rr, fw.srcFd.BlockSize)
				if err != nil {
					return errors.Wrap(err, "error writing referenced data")
				}
				w.Flush()
			}
		case lfs.End:
			return lfs.ErrEOF
		default:
			return errors.New("file writer - invalid header")
		}
	}
	return nil
}

func (fw FileWriter) newTempFile(offset int64) error {
	dstDir := viper.GetString("destination")
	tmpF, err := ioutil.TempFile(dstDir, fw.srcFd.RelPath+".*."+fw.senderID.String())
	if err != nil {
		return errors.Wrap(err, "unable to create temporary file")
	}
	log.Debug().
		Str("file name", tmpF.Name()).
		Int64("offset", offset).
		Int("temp files count", len(fw.fileMap)).
		Msg("file writer - DIFF opening temporary file ++++++++++++++++++++++++++++++++++++++")

	fw.fileMap[offset] = &tmpFile{
		path:    tmpF.Name(),
		f:       tmpF,
		w:       bufio.NewWriter(io.Writer(tmpF)),
		dataBuf: make(map[int64]*lfs.DataDesc),
	}

	return nil
}
