package lfs

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestHeaders(t *testing.T) {
	h1 := &Header{
		Flag:      int16(Data),
		FileIndex: 3,
		Offset:    4096,
		Seq:       5,
		Len:       10,
	}
	t.Logf("Using: %+v\n", h1)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, h1)
	if err != nil {
		t.Fatalf("write failed %s\n", err.Error())
	}
	if buf.Len() != HeaderSize {
		t.Fatalf("header size: %d does not equal constant: %d", buf.Len(), HeaderSize)
	}
	h2 := &Header{}
	err = binary.Read(buf, binary.BigEndian, h2)
	if err != nil {
		t.Fatalf("write failed %s\n", err.Error())
	}
	if !reflect.DeepEqual(h1, h2) {
		t.Fatalf("Headers do not match \n%+v\n%+v\n", h1, h2)
	}

}

func TestBytesWriter(t *testing.T) {
	d1 := []byte("“For us, at the Highest Possible Level, there is nothing left to do in this Universe, \nand to create another Universe, in my opinion, would be in extremely poor taste.”")
	dd1 := NewDataDesc(0, 1, 2, 1)
	n, err := dd1.Write(d1)
	t.Logf("encoded %d bytes\n", n)
	if err != nil {
		t.Fatalf("write failed %s\n", err.Error())
	}
	t.Logf("Selializing - index: %d, offset: %d, seq: %d\n", dd1.fileIndex, dd1.offset, dd1.seq)
	s, err := dd1.Serialize()
	if err != nil {
		t.Fatalf("serialize failed %s\n", err.Error())
	}
	t.Logf("Serialized data:\n %+v\n", dd1.data)
	dd2, err := Deserialize(s)
	if err != nil {
		t.Fatalf("deserialize failed %s\n", err.Error())
	}
	t.Logf("Deserialized - index: %d, offset: %d, seq: %d\n", dd2.fileIndex, dd2.offset, dd2.seq)
	t.Logf("Deserialized data:\n %+v\n", dd2.data)
	h := new(Header)
	r := bytes.NewReader(dd2.Bytes())
	err = binary.Read(r, binary.BigEndian, h)
	if err != nil {
		t.Fatalf("read failed %s\n", err.Error())
	}
	t.Logf("\nHeader: %+v\n", h)
	d2 := make([]byte, h.Len)
	err = binary.Read(r, binary.BigEndian, d2)
	if string(d1) != string(d2) {
		t.Fatalf("\nd1: %s\nd2: %s\n do not match", d1, d2)
	}

}
