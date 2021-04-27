package core

import (
	"bytes"
	"hash/adler32"
	"io"
	"testing"
)

const blockSize = 8

func TestRhash(t *testing.T) {
	data := []byte("abcdefghijklmnop")
	r := bytes.NewReader(data)
	buf := make([]byte, blockSize)

	n, err := io.ReadFull(r, buf)
	if n == 0 {
		if err != nil {
			t.Fatal("failed to ready data")
		}
		t.Fatal("read zero bytes")
	}

	rh := Pour(blockSize)
	n, err = rh.Write(buf)
	if n != blockSize || err != nil {
		t.Fatal("failed to initialize rolling hash")
	}

	a32 := adler32.New()
	a32.Write(buf)
	if a32.Sum32() != rh.Sum32() {
		t.Fatal("rolling hash does not match adler32 digest")
	}

	n, err = io.ReadFull(r, buf)
	if n == 0 {
		if err != nil {
			t.Fatal("failed to ready data")
		}
		t.Fatal("read zero bytes")
	}

	rn := rune('a')
	for i := 0; i < 8; i++ {
		b := rh.Roll(buf[i])
		//t.Logf("%s left", string(b))
		if b != byte(rn) {
			t.Fatalf("rolling hash failed, %v != %v", string(b), rn)
		}
		rn++
		window := data[i+1 : i+9]
		t.Logf("rh window %s, a32 window %s", rh.window, window)
		a32.Reset()
		a32.Write(window)
		aSum := a32.Sum32()
		rSum := rh.Sum32()
		if aSum != rSum {
			t.Fatalf("rolling hash fail, adler32(%v) = %d != radler32(%v) = %d", string(window), aSum, string(rh.window), rSum)
		}
	}
}
