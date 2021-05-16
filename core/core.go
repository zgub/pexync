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
	offset, limit, streams int64           // meta data required for reconstruction
	uuid                   uuid.UUID       // meta data
	fileList               []*lfs.FileDesc // meta data
	fileDesc               *lfs.FileDesc   // meta data
	dataDesc               *lfs.DataDesc   // binary (actual) data
}

func NewINI(uuid uuid.UUID, list []*lfs.FileDesc) *Message {
	return &Message{
		flag:     INI,
		uuid:     uuid,
		fileList: list,
	}
}

func NewRSQ(uuid uuid.UUID, fd *lfs.FileDesc, offset, limit, streams int64) *Message {
	return &Message{
		uuid:     uuid,
		flag:     RSQ,
		offset:   offset,
		limit:    limit,
		fileDesc: fd,
	}
}

func NewFIN(uuid uuid.UUID) *Message {
	return &Message{
		uuid: uuid,
		flag: FIN,
	}
}

func NewHashRequest(fd *lfs.FileDesc) *Message {
	return &Message{
		flag:     HSH,
		fileDesc: fd,
	}
}

func NewWSQ(dd *lfs.DataDesc) *Message {
	return &Message{
		flag:     WSQ,
		dataDesc: dd,
	}
}

func NewDataWSQ(dd *lfs.DataDesc, fd *lfs.FileDesc) *Message {
	return &Message{
		flag:     WSQ,
		fileDesc: fd,
		dataDesc: dd,
	}
}

func (m *Message) SetFlag(f Flag) {
	m.flag = f
}

func (m *Message) GetFlag() Flag {
	return m.flag
}

func NewACK() *Message {
	return &Message{
		flag: ACK,
	}
}

func (m *Message) GetList() []*lfs.FileDesc {
	return m.fileList
}

func (m *Message) GetUuid() uuid.UUID {
	return m.uuid
}

func (m *Message) GetFileDesc() *lfs.FileDesc {
	return m.fileDesc
}

func (m *Message) GetDataDesc() *lfs.DataDesc {
	return m.dataDesc
}

func (m *Message) GetOffset() int64 {
	return m.offset
}

func (m *Message) GetLimit() int64 {
	return m.limit
}

func (m *Message) GetStreamCount() int64 {
	return m.streams
}
