package lfs

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"

	//"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type State int16

const (
	Missing State = iota // no file on the receiver side
	Diff                 // file exists but has different file size
	Meta                 // file exists, has the same filesize but different meta
	Skip                 // file exists and matches
)

var ErrEOF = errors.New("end of file transmission")

var fileStatus = [...]string{
	"MISS",
	"DIFF",
	"META",
	"SKIP",
}

func (s State) String() string {
	return fileStatus[s]
}

type Flag int16

const (
	Data  Flag = iota // Data header
	Index             // index header
	End               // end header
)

var headerFlags = [...]string{
	"DATA",
	"INDEX",
	"END",
}

func (f Flag) String() string {
	return headerFlags[f]
}

const (
	HeaderSize = 34
)

// common errors, lazy to type
const (
	absPathError = "error listing directory - failed while determining the absolute path"
	walkError    = "error listing directory"
)

// lets talk 64bit only to keep this simple
type Header struct {
	Flag      int16 // true - data / false - index
	FileIndex int64 // global header only
	Offset    int64 // global header only
	Seq       int64 // for proper reconstruction
	Streams   int64 // number of simultaneous data streams
	Len       int64
}

type DataDesc struct {
	mode                   Flag // true - writing data / false - writing index data
	offset, seq, fileIndex int64
	iBuff                  []int64       // intermediate index buffer
	readBuf                *bytes.Buffer // intermediate data buffer
	data                   *bytes.Buffer
	streams                int64 // ccIo
	//len                    int64         //is ths really neccessary?
}

func NewDataDesc(fileIndex, offset, sequence, streams int64) *DataDesc {
	return &DataDesc{
		fileIndex: fileIndex,
		readBuf:   new(bytes.Buffer),
		data:      new(bytes.Buffer),
		offset:    offset,
		seq:       sequence,
		streams:   streams,
	}
}

func (dd *DataDesc) Seq() int64 {
	return dd.seq
}

func (dd *DataDesc) Offset() int64 {
	return dd.offset
}

func (dd *DataDesc) FileIndex() int64 {
	return dd.fileIndex
}

func (dd *DataDesc) Bytes() []byte {
	return dd.data.Bytes()
}

func (dd *DataDesc) Write(b []byte) (int, error) {
	header := &Header{
		Flag: int16(Data),
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
	if dd.mode == Index {
		err := dd.flush()
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.mode = Data
	}
	err := dd.readBuf.WriteByte(b)
	if err != nil {
		return errors.Wrap(err, "unable to encode data")
	}
	return nil
}

func (dd *DataDesc) WriteIndex(i int64) error {
	if dd.mode == Data {
		err := dd.flush()
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.mode = Index
	}
	dd.iBuff = append(dd.iBuff, i)
	return nil
}

func (dd *DataDesc) flush() error {
	// we were (probably) witing data, flush them
	if dd.mode == Data && dd.readBuf.Len() != 0 {
		// header first
		header := &Header{
			Flag: int16(Data),
			Len:  int64(dd.readBuf.Len()),
		}
		// write header
		err := binary.Write(dd.data, binary.BigEndian, header)
		if err != nil {
			return errors.Wrap(err, "unable to encode data header")
		}
		// flush the intermediate data buffer to main buffer if there is somethign to write
		_, err = dd.data.Write(dd.readBuf.Bytes())
		if err != nil {
			return errors.Wrap(err, "unable to encode data")
		}
		dd.iBuff = make([]int64, 0)
	} else if dd.mode == Index && len(dd.iBuff) != 0 {
		// we were writing indexes, flush them
		header := &Header{
			Flag: int16(Index),
			Len:  int64(len(dd.iBuff)),
		}
		// write header
		err := binary.Write(dd.data, binary.BigEndian, header)
		if err != nil {
			return errors.Wrap(err, "unable to encode index header")
		}
		// write data, but only if there is something to write
		err = binary.Write(dd.data, binary.BigEndian, dd.iBuff)
		if err != nil {
			return errors.Wrap(err, "unable to encode indexes")
		}
		dd.readBuf.Reset()
	}
	return nil
}

func (dd *DataDesc) Len() int64 {
	// flushed data (bytes) + number of int64 * 8 ("bytes") + intermediate data (bytes)
	// not exact propably, if binary optimizes
	//return int64(dd.data.Len() + (len(dd.iBuff) * 8) + dd.readBuf.Len())
	return int64(dd.readBuf.Len())
}

func (dd *DataDesc) Serialize() ([]byte, error) {
	// flush any remainung data
	err := dd.flush()
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode data")
	}
	// global header
	header := &Header{
		FileIndex: dd.fileIndex,
		Streams:   int64(dd.streams),
		Offset:    dd.offset,
		Seq:       dd.seq,
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

func Deserialize(p []byte) (*DataDesc, error) {
	header := new(Header)
	r := bytes.NewReader(p)
	err := binary.Read(r, binary.BigEndian, header)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize data")
	}
	dd := &DataDesc{
		fileIndex: header.FileIndex,
		streams:   header.Streams,
		offset:    header.Offset,
		seq:       header.Seq,
		data:      bytes.NewBuffer(p[HeaderSize:]),
		//len:       header.Len,
	}
	return dd, nil
}

func (dd *DataDesc) MarkAsLast() error {
	dd.flush()
	header := &Header{
		Flag: int16(End),
	}
	err := binary.Write(dd.data, binary.BigEndian, header)
	if err != nil {
		return errors.Wrap(err, "unable to encode data")
	}
	return nil
}

type FileDesc struct {
	IsDir     bool
	State     State
	Uid, Gid  uint32
	Idx       int64
	BlockSize int64
	FileSize  uint64
	Sha1      []byte
	Weak      []uint32
	RelPath   string
	Prefix    string
	FileName  string
	Modified  time.Time
	Mode      os.FileMode
}

func (fd *FileDesc) SetBlockSize() {
	// fetch the config value, which has priority if changed and remains 700 if filesize is sma;;
	fd.BlockSize = viper.GetInt64("block_size")
	// if the file size is big enoigh anf the value is still default
	if fd.FileSize > 490000 && fd.BlockSize == 700 {
		// stolen from rsync doc :)
		sqrt := math.Sqrt(float64(fd.FileSize))
		fd.BlockSize = int64(math.Round(sqrt))
		if fd.BlockSize > 131072 {
			fd.BlockSize = 131072
		}
	}

	if fd.FileSize < 700 {
		fd.BlockSize = int64(fd.FileSize)
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

	dest, err := filepath.Abs(viper.GetString("destination"))
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
				Msg("walkdir - skipping destination")
			return filepath.SkipDir
		}

		info, err := entry.Info()
		if err != nil {
			return errors.Wrap(err, "file stat info failed")
		}

		//stat := info.Sys().(*syscall.Stat_t)

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
			Msg("walkdir - parsing fs entry")

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
				//Uid:      stat.Uid,
				//Gid:      stat.Gid,
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
