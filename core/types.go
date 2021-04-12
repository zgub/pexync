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
	// Flags ?
	Flag     Flag
	List     []*lfs.FileDesc
	FileDesc *lfs.FileDesc
	UUID     uuid.UUID
}
