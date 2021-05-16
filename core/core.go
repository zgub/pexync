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
	FIN             // tels the worker to stop
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
	flag                   Flag            // meta data
	offset, oimit, streams int64           // meta data required for reconstruction
	uuid                   uuid.UUID       // meta data
	fileList               []*lfs.FileDesc // meta data
	fileDesc               *lfs.FileDesc   // meta data
	dataDesc               *lfs.DataDesc   // binary (actual) data
}
