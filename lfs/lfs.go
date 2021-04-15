package lfs

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type State int

const (
	Missing State = iota // no file on the receiver side
	Diff                 // file exists but do not match
	Skip                 // file exists and matches
)

const (
	DataFlag  bool = true
	IndexFlag bool = false
)

var fileStatus = [...]string{
	"MISS",
	"DIFF",
	"SKIP",
}

func (s State) String() string {
	return fileStatus[s]
}

// common errors, lazy to type
const (
	absPathError = "error listing directory - failed while determining the absolute path"
	walkError    = "error listing directory"
)

// lets talk 64bit only to keep this simple
type Header struct {
	FileIndex int64 // global header only
	Offset    int64 // global header only
	Seq       int64 // for proper reconstruction
	Flag      bool  // true - data / false - index
	Len       int64
}
type DataDesc struct {
	readBuf                *bytes.Buffer // intermediate data buffer
	iBuff                  []int64       // intermediate index buffer
	writingData            bool          // true - writing data / false - writing index data
	data                   *bytes.Buffer
	offset, seq, fileIndex int64
}

func NewDataDesc(fileIndex, offset, sequence int64) *DataDesc {
	return &DataDesc{
		fileIndex: fileIndex,
		readBuf:   new(bytes.Buffer),
		data:      new(bytes.Buffer),
		offset:    offset,
		seq:       sequence,
	}
}

func (dd *DataDesc) Write(b []byte) (int, error) {
	header := &Header{
		Flag: DataFlag,
		Len:  int64(len(b)),
	}
	err := binary.Write(dd.data, binary.BigEndian, header)
	if err != nil {
		return 0, errors.Wrap(err, "unable to encode data")
	}
	n, err := dd.data.Write(b)
	if err != nil {
		return 0, errors.Wrap(err, "unable to encode data")
	}
	return n, nil
}

func (dd *DataDesc) WriteByte(b byte) error {
	// make sure to write the header when it is the first data write and writingata is true
	if !dd.writingData {
		err := dd.flush()
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.writingData = true
	}
	err := dd.readBuf.WriteByte(b)
	if err != nil {
		return errors.Wrap(err, "unable to encode data")
	}
	return nil
}

func (dd *DataDesc) WriteIndex(i int64) error {
	if dd.writingData {
		err := dd.flush()
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.writingData = false
	}
	dd.iBuff = append(dd.iBuff, i)
	return nil
}

func (dd *DataDesc) flush() error {
	// we were (probably) witing data, flush them
	if dd.writingData && dd.readBuf.Len() != 0 {
		// header first
		header := &Header{
			Flag: DataFlag,
			Len:  int64(dd.readBuf.Len()),
		}
		// write header
		err := binary.Write(dd.data, binary.BigEndian, header)
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		// flush the intermediate data buffer to main buffer if there is somethign to write
		_, err = dd.data.Write(dd.readBuf.Bytes())
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.readBuf.Reset()
	} else if len(dd.iBuff) != 0 {
		// we were writing indexes, flush them
		header := &Header{
			Flag: IndexFlag,
			Len:  int64(len(dd.iBuff)),
		}
		// write header
		err := binary.Write(dd.data, binary.BigEndian, header)
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		// write data, but only if there is something to write
		err = binary.Write(dd.data, binary.BigEndian, dd.iBuff)
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.iBuff = make([]int64, 0)
	}
	return nil
}

func (dd *DataDesc) Len() int {
	// flushed data (bytes) + number of int64 /8 ("bytes") + intermediate data (bytes)
	// not exact propably, if binary optimizes
	return dd.data.Len() + (len(dd.iBuff) / 8) + dd.readBuf.Len()
}

func (dd *DataDesc) Serialize() ([]byte, error) {
	log.Debug().
		Int64("length", int64(dd.Len())).
		Msg("serializing")
	// flush any remainung data
	err := dd.flush()
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode data")
	}
	// global header
	header := &Header{
		FileIndex: dd.fileIndex,
		Offset:    dd.offset,
		Len:       int64(dd.data.Len()),
	}
	buf := new(bytes.Buffer)
	// write global header to new buffer
	err = binary.Write(buf, binary.BigEndian, header)
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode data header")
	}
	// wite data
	_, err = buf.Write(dd.data.Bytes())
	return buf.Bytes(), nil
}

type FileDesc struct {
	Idx       int64
	State     State
	IsDir     bool
	RelPath   string
	Prefix    string
	FileName  string
	FileSize  uint64
	BlockSize int
	Modified  time.Time
	Mode      os.FileMode
	Uid, Gid  uint32
	Sha1      []byte
	Weak      []uint32
}

func (fd *FileDesc) SetBlockSize() {
	// fetch the config value, which has priority if changed and remains 700 if filesize is sma;;
	fd.BlockSize = viper.GetInt("block_size")
	// if the file size is big enoigh anf the value is still default
	if fd.FileSize > 490000 && fd.BlockSize == 700 {
		// stolen from rsync doc :)
		sqrt := math.Sqrt(float64(fd.FileSize))
		fd.BlockSize = int(math.Round(sqrt))
		if fd.BlockSize > 131072 {
			fd.BlockSize = 131072
		}
	}

	if fd.FileSize < 700 {
		fd.BlockSize = int(fd.FileSize)
	}

}

func ParseDir(walkDir string) ([]*FileDesc, error) {
	//walkPath = prefix + walkPath
	var list []*FileDesc

	// don't do walk over abs path, makes comparing more difficult
	walkDirAbs, err := filepath.Abs(walkDir)
	if err != nil {
		return nil, errors.Wrap(err, absPathError)
	}

	// filepath index to refer tol later
	var idx int64

	// avoid endless recursive deadend
	dest, err := filepath.Abs(viper.GetString("local_destination"))
	if err != nil {
		return nil, errors.Wrap(err, absPathError)
	}

	err = filepath.WalkDir(walkDir, func(path string, entry os.DirEntry, err error) error {

		if err != nil {
			return errors.Wrap(err, walkError)
		}

		// skip destination folder if it's located within the source
		absPath, err := filepath.Abs(path)
		if err != nil {
			return errors.Wrap(err, absPathError)
		}

		// not cheap, but it's not done that often
		if absPath == dest && walkDirAbs != dest {
			log.Trace().
				Str("path", path).
				Str("destination", dest).
				Msg("skipping destination")
			return filepath.SkipDir
		}

		info, err := entry.Info()
		if err != nil {
			return errors.Wrap(err, "file stat info failed")
		}

		stat := info.Sys().(*syscall.Stat_t)

		relPath, err := filepath.Rel(walkDir, path)
		if err != nil {
			return errors.Wrap(err, "failed to determine relative file path")
		}
		prefix := filepath.Dir(absPath)
		log.Trace().
			Int64("file index", idx).
			Str("path", path).
			Str("prefix path", prefix).
			Bool("is dir", entry.IsDir()).
			Msg("parsing fs entry")

		if relPath != "." {
			fileDesc := &FileDesc{
				Idx:      idx,
				IsDir:    entry.IsDir(),
				RelPath:  relPath,
				Prefix:   prefix,
				FileName: entry.Name(),
				FileSize: uint64(info.Size()),
				Modified: info.ModTime(),
				Mode:     info.Mode(),
				Uid:      stat.Uid,
				Gid:      stat.Gid,
			}

			list = append(list, fileDesc)
		}

		// increment file index
		idx++
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "error listing directory")
	}

	return list, nil
}

func DummyWriter(b []byte, name string) error {
	var (
		headerCnt, dataCnt, indexCnt int64
	)
	header := new(Header)
	r := bytes.NewReader(b)
	// read global
	err := binary.Read(r, binary.BigEndian, header)
	if err != nil {
		return errors.Wrap(err, "unable to dummy read ")
	}
	headerCnt++
	log.Trace().
		Int64("file index", header.FileIndex).
		Int64("offset", header.Offset).
		Int64("section data length", header.Len).
		Str("filename", name).
		Msg("DummyWriter - section global header")
	for {
		// read data header
		err = binary.Read(r, binary.BigEndian, header)
		if err != nil {
			if err == io.EOF {
				log.Trace().
					Msg("DummyWriter - EOF - while reading global header")
				break
			} else {
				return errors.Wrap(err, "DummyWritter - error reading header data")
			}
		}
		headerCnt++
		//spew.Dump(header)
		//dLen := header.Len
		//flag := header.Flag
		// DataFlag = true
		if header.Flag {
			// true = data
			/*log.Trace().
			Int64("length", dLen).
			Str("filename", name).
			Msg("DummyWritter - byte data header")*/
			dataBuf := make([]byte, header.Len)
			err = binary.Read(r, binary.BigEndian, dataBuf)
			if err != nil {
				if err == io.EOF {
					log.Trace().
						Msg("DummyWriter - EOF - while reading data")
					break
				} else {
					return errors.Wrap(err, "DummyWriter - error reading data")
				}
			}
			dataCnt += header.Len
			/*log.Trace().
			Int("length", len(dataBuf)).
			Str("filename", name).
			Msg("DummyWritter - byte data processed")*/
		} else {
			/*log.Trace().
			Int64("length", dLen).
			Str("filename", name).
			Msg("DummyWriter - index data header")*/
			indexes := make([]int64, header.Len)
			err = binary.Read(r, binary.BigEndian, indexes)
			if err != nil {
				if err == io.EOF {
					log.Trace().
						Msg("DummyWritter - EOF - while reading indexes")
					break
				} else {
					return errors.Wrap(err, "DummyWriter - error reading index data")
				}

			}
			indexCnt += header.Len
			/*log.Trace().
			Int("length", len(indexes)).
			Str("filename", name).
			Msg("DummyWriter - index data processed")*/
		}
	}
	log.Trace().
		Int64("headers", headerCnt).
		Int64("bytes", dataCnt).
		Int64("indexes", indexCnt).
		Int64("file index", header.FileIndex).
		Str("filename", name).
		Msg("decoded")
	return nil
}
