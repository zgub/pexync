package cmd

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"unicode/utf8"

	"github.com/spf13/viper"
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

const (
	srcD = "../test/"
	dstD = "../Xync"
)

func (t testFileType) String() string {
	return testFileTypes[t]
}

func createTestFile(dir string, blockSize, blockCount int, t testFileType) (string, error) {

	path := fmt.Sprintf(dir+"/%dx%d-test-data-%s", blockCount, blockSize, t.String())
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("huh? %s\n", err.Error())
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

func TestMissingLocalSync(t *testing.T) {
	testFiles := make([]string, 3)
	var err error

	testFiles[0], err = createTestFile(srcD, 700, 3, AABBCC)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	testFiles[1], err = createTestFile(srcD, 700, 3, BBCCDD)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	testFiles[2], err = createTestFile(srcD, 00, 3, AACCEE)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	viper.Set("source", srcD)
	viper.Set("destination", dstD)

	startLocalSync()

	h := md5.New()
	for _, p := range testFiles {

		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("failed to open file: %s", err.Error())
		}
		if _, err := io.Copy(h, f); err != nil {
			t.Fatalf("MD5 has read failed: %s", err.Error())
		}

		srcSum := h.Sum(nil)
		h.Reset()
		f.Close()

		name := filepath.Base(p)
		f, err = os.Open("../Xync/" + name)
		if err != nil {
			t.Fatalf("failed to open file: %s", err.Error())
		}
		if _, err := io.Copy(h, f); err != nil {
			t.Fatalf("MD5 has read failed: %s", err.Error())
		}
		dstSum := h.Sum(nil)

		if !reflect.DeepEqual(srcSum, dstSum) {
			t.Fatalf("File content does not math %s : %s", p, "../Xync/"+name)
		}
		err = os.Remove(p)
		if err != nil {
			t.Fatalf("unable to remove file: %s", err.Error())
		}
		err = os.Remove("../Xync/" + name)
		if err != nil {
			t.Fatalf("unable to remove file: %s", err.Error())
		}
		h.Reset()
		f.Close()
	}

}

func TestDiffLocalSync(t *testing.T) {
	srcF, err := createTestFile(srcD, 700, 5, AABBCC)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	dstF, err = createTestFile(dstD, 700, 3, AACCEE)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	if os.Rename(dstD)

	viper.Set("source", "../test")
	viper.Set("destination", "../Xync")

	startLocalSync()
}
