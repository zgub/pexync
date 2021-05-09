package test

import (
	"bufio"
	"fmt"
	"hash/adler32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"
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

func CreateTestFile(dir, name string, blockSize, blockCount int, t testFileType) (string, error) {

	var p string
	if name == "" {
		name = fmt.Sprintf("%dx%d-test-file-%s", blockCount, blockSize, t.String())
		p = filepath.Join(dir, name)
		fmt.Printf("path: %s\n", p)
	} else {
		p = filepath.Join(dir, name)
		fmt.Printf("path: %s\n", p)
	}
	f, err := os.Create(p)
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
			_, err = bw.Write([]byte("\n"))
			if err != nil {
				return "", err
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
			_, err = bw.Write([]byte("\n"))
			if err != nil {
				return "", err
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
			_, err = bw.Write([]byte("\n"))
			if err != nil {
				return "", err
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
	return p, nil
}

type TestData struct {
	Src     [][]byte
	Dst     [][]byte
	HitList []int
	HashMap map[uint32][]byte
}

func CreateRandPair(bs, l int64) ([]int, error) {
	srcF, err := os.Create("testfiles/rand-test-file")
	if err != nil {
		return nil, err
	}
	defer srcF.Close()
	dstF, err := os.Create("Xync/rand-test-file")
	if err != nil {
		return nil, err
	}
	defer dstF.Close()
	td, err := randPair(bs, l)
	if err != nil {
		return nil, err
	}

	srcbw := bufio.NewWriter(srcF)
	dstbw := bufio.NewWriter(dstF)

	for _, b := range td.Src {
		_, err := srcbw.Write(b)
		if err != nil {
			return nil, err
		}
	}

	for _, b := range td.Dst {
		_, err = dstbw.Write(b)
		if err != nil {
			return nil, err
		}
	}

	err = srcbw.Flush()
	if err != nil {
		return nil, err
	}
	err = dstbw.Flush()
	if err != nil {
		return nil, err
	}
	srcF.Sync()
	dstF.Sync()

	return td.HitList, nil
}

func randPair(bs int64, l int64) (*TestData, error) {

	rand.Seed(time.Now().UnixNano())

	a32 := adler32.New()
	t := &TestData{
		HashMap: make(map[uint32][]byte),
	}

	for i := int64(0); i < l; i++ {
		a32.Reset()
		d, err := getRandBytes(int(bs))
		if err != nil {
			return nil, err
		}
		a32.Write(d)
		t.HashMap[a32.Sum32()] = d
		t.Dst = append(t.Dst, d)
	}

	for i := int64(0); i < l; i++ {
		r := rand.Intn(int(bs))
		rd, err := getRandBytes(r)
		if err != nil {
			return nil, err
		}
		t.Src = append(t.Src, rd)
		rp := rand.Intn(int(l))
		t.HitList = append(t.HitList, rp)
		t.Src = append(t.Src, t.Dst[rp])
	}
	r := rand.Intn(int(bs))
	rd, err := getRandBytes(r)
	if err != nil {
		return nil, err
	}
	t.Src = append(t.Src, rd)

	return t, nil
}

func getRandBytes(l int) ([]byte, error) {
	buf := make([]byte, l)
	_, err := rand.Read(buf)
	return buf, err
}
