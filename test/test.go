package test

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"unicode/utf8"
)

type testFileType int

const (
	AABBCC testFileType = iota
	BBCCDD
	AACCEE
)

var testFileTypes = [...]string{
	"AABBCC",
	"BBCCDD",
	"AACCEE",
}

func (t testFileType) String() string {
	return testFileTypes[t]
}

func CreateTestFile(dir string, blockSize, blockCount int, t testFileType) (string, error) {

	path := fmt.Sprintf(dir+"/%dx%d-test-file-%s", blockCount, blockSize, t.String())
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	bw := bufio.NewWriter(io.Writer(f))
	switch t {
	case AABBCC:
		rn := rune('a')
		buf := make([]byte, utf8.UTFMax)
		for block := 0; block < blockCount; block++ {
			for byte := 0; byte < blockSize; byte++ {
				n := utf8.EncodeRune(buf, rn)
				buf = buf[:n]
				_, err = bw.Write(buf)
				if err != nil {
					return "", err
				}
			}
			rn++
		}
	case BBCCDD:
		rn := rune('b')
		buf := make([]byte, utf8.UTFMax)
		for block := 0; block < blockCount; block++ {
			for byte := 0; byte < blockSize; byte++ {
				n := utf8.EncodeRune(buf, rn)
				buf = buf[:n]
				_, err = bw.Write(buf)
				if err != nil {
					return "", err
				}
			}
			rn++
		}
	case AACCEE:
		rn := rune('a')
		buf := make([]byte, utf8.UTFMax)
		for block := 0; block < blockCount; block++ {
			for byte := 0; byte < blockSize; byte++ {
				n := utf8.EncodeRune(buf, rn)
				buf = buf[:n]
				_, err = bw.Write(buf)
				if err != nil {
					return "", err
				}
			}
			rn++
			rn++
		}
	}

	for i := 0; i < rand.Intn(10); i++ {
		_, err = bw.Write([]byte("END"))
		if err != nil {
			return "", err
		}
	}

	bw.Flush()
	f.Sync()
	return path, nil
}
