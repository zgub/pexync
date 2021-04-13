package core

import (
	"github.com/google/uuid"
	"github.com/zgub/pexync/lfs"
)

// "API" :)

type Flag int

const (
	NIL Flag = iota // no flag set
	RST             // reset, (re)initialize), hello
	SUM             // checksum data from receiver
	DTA             // data from file reader
	ERR             // error
	FIN             // done, disconnect
)

var messageTypes = [...]string{
	"NIL",
	"RST",
	"SUM",
	"DTA",
	"ERR",
	"FIN",
}

func (f Flag) String() string {
	return messageTypes[f]
}

type Message struct {
	Flag     Flag            // meta data
	List     []*lfs.FileDesc // meta data
	FileDesc *lfs.FileDesc   // meta data
	DataDesc *lfs.DataDesc   // binary (actual) data
	UUID     uuid.UUID       // meta data
}
