package core

import (
	"github.com/google/uuid"
	"github.com/zgub/pexync/lfs"
)

// "API" :)

type Flag int

// not all are strictly neccessary, but concept is concept :-/
const (
	NIL Flag = iota // no flag set
	INI             // reset, (re)initialize), hello
	HSH             // calculate hashes
	SUM             // checksum data from receiver
	RSQ             // read sequence
	WSQ             // write sequence
	ERR             // error
	FIN             // tels the woker to stop
	ACK             // just ACK
)

var messageTypes = [...]string{
	"NIL",
	"INI",
	"HSH",
	"SUM",
	"RSQ",
	"WSQ",
	"ERR",
	"FIN",
	"ACK",
}

func (f Flag) String() string {
	return messageTypes[f]
}

type Message struct {
	Flag          Flag            // meta data
	FileList      []*lfs.FileDesc // meta data
	FileDesc      *lfs.FileDesc   // meta data
	DataDesc      *lfs.DataDesc   // binary (actual) data
	UUID          uuid.UUID       // meta data
	Offset, Limit int64           // meta data required for reconstruction
}
