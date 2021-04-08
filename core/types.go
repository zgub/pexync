package core

import (
	"github.com/google/uuid"
	"github.com/zgub/pexync/lfs"
)

// "API" :)

type Flag int

const (
	RST Flag = iota // reset, (re)initialize), hello
	ACK             // acknowledge, everything is ok
	FLS             // initial filelist from sender
	DTA             // data from receiver
	ERR             // error
	FIN             // done, disconnect
)

var messageTypes = [...]string{
	"RST",
	"ACK",
	"FLS",
	"DTA",
	"ERR",
	"FIN",
}

func (f Flag) String() string {
	return messageTypes[f]
}

type Message struct {
	// Flags ?
	Flag Flag
	List []*lfs.FileDesc
	File *lfs.FileDesc
	UUID uuid.UUID
}
