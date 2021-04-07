package core

import (
	"github.com/google/uuid"
	"github.com/zgub/pexync/lfs"
)

type Error string

// errors
const (
	UnknownMessage Error = "unknown message"
	NotImplemented Error = "functionality not (yet) implemented"
	Timeout        Error = "timeout reached"
	NoError        Error = ""
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
	Flag  Flag
	List  []*lfs.FileDesc
	File  *lfs.FileDesc
	Error *Error
	UUID  uuid.UUID
}
