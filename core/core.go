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
	ADD             // new file in monitor mode
	UPD             // update existing file
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
	"ADD",
	"UPD",
	"ERR",
	"FIN",
	"ACK",
	"FEV",
	"WAI",
}

func (f Flag) String() string {
	return messageTypes[f]
}

type Message struct {
	Flag                   Flag            // meta data
	Offset, Limit, Streams int64           // meta data required for reconstruction
	SenderID               uuid.UUID       // meta data
	FileList               []*lfs.FileDesc // meta data
	FileDesc               *lfs.FileDesc   // meta data
	DataDesc               *lfs.DataDesc   // binary (actual) data
	//FileLock               *sync.Mutex
}

func NewINI(senderID uuid.UUID, list []*lfs.FileDesc) *Message {
	return &Message{
		Flag:     INI,
		SenderID: senderID,
		FileList: list,
	}
}

func NewADD(senderID uuid.UUID, fd *lfs.FileDesc) *Message {
	return &Message{
		Flag:     ADD,
		SenderID: senderID,
		FileDesc: fd,
	}
}

func NewUPD(senderID uuid.UUID, fd *lfs.FileDesc) *Message {
	return &Message{
		Flag:     UPD,
		SenderID: senderID,
		FileDesc: fd,
	}

}

/*
func NewAsyncRSQ(senderID uuid.UUID, fd *lfs.FileDesc, offset, limit, streams int64, fLock *sync.Mutex) *Message {
	if streams == 0 {
		panic("new rsq: zero data streams")
	}
	log.Trace().
		Str("filename", fd.FileName).
		Int64("file size", fd.FileSize).
		Msg("Async RSQ REQUEST")
	return &Message{
		SenderID: senderID,
		Flag:     RSQ,
		Offset:   offset,
		Limit:    limit,
		Streams:  streams,
		FileDesc: fd,
		FileLock: fLock,
	}
}
*/

func NewRSQ(senderID uuid.UUID, fd *lfs.FileDesc, offset, limit, streams int64) *Message {
	if streams == 0 {
		panic("new rsq: zero data streams")
	}
	return &Message{
		SenderID: senderID,
		Flag:     RSQ,
		Offset:   offset,
		Limit:    limit,
		Streams:  streams,
		FileDesc: fd,
	}
}

func NewFIN(senderID uuid.UUID) *Message {
	return &Message{
		SenderID: senderID,
		Flag:     FIN,
	}
}

func NewHashRequest(fd *lfs.FileDesc) *Message {
	return &Message{
		Flag:     HSH,
		FileDesc: fd,
	}
}

func NewWSQ(dd *lfs.DataDesc) *Message {
	return &Message{
		Flag:     WSQ,
		DataDesc: dd,
	}
}

func NewDataWSQ(dd *lfs.DataDesc, fd *lfs.FileDesc) *Message {
	return &Message{
		Flag:     WSQ,
		FileDesc: fd,
		DataDesc: dd,
	}
}

func NewACK() *Message {
	return &Message{
		Flag: ACK,
	}
}

func (m *Message) SetFlag(f Flag) {
	m.Flag = f
}

func (m *Message) SetFileDesc(fd *lfs.FileDesc) {
	m.FileDesc = fd
}

func (m *Message) GetFlag() Flag {
	return m.Flag
}

func (m *Message) GetList() []*lfs.FileDesc {
	return m.FileList
}

func (m *Message) GetID() uuid.UUID {
	return m.SenderID
}

func (m *Message) GetFileDesc() *lfs.FileDesc {
	return m.FileDesc
}

func (m *Message) GetDataDesc() *lfs.DataDesc {
	return m.DataDesc
}

func (m *Message) GetOffset() int64 {
	return m.Offset
}

func (m *Message) GetLimit() int64 {
	return m.Limit
}

func (m *Message) GetStreamCount() int64 {
	return m.Streams
}
