package cmd

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
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
	dstD = "../Xync/"
)

func (t testFileType) String() string {
	return testFileTypes[t]
}

func createTestFile(dir string, blockSize, blockCount int, t testFileType) (string, error) {

	path := fmt.Sprintf(dir+"/%dx%d-test-data-%s", blockCount, blockSize, t.String())
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

	testFiles[2], err = createTestFile(srcD, 700, 3, AACCEE)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	viper.Set("source", srcD)
	viper.Set("destination", dstD)

	startLocalSync()

	for _, fn := range testFiles {
		bfn := filepath.Base(fn)
		eq, err := compare(srcD+bfn, dstD+bfn)
		if err != nil {
			t.Fatalf("unable to comapre files: %s", err.Error())
		}
		if !eq {
			t.Fatalf("\n *** %s \n *** %s \n *** not equal", srcD+bfn, dstD+bfn)
		}
	}
	if err = cleanup(testFiles); err != nil {
		t.Errorf("unable to remove test files")
	}

}

/*
func TestDiffLocalSync(t *testing.T) {
	srcF, err := createTestFile(srcD, 700, 5, AABBCC)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	dstF, err := createTestFile(dstD, 700, 3, AACCEE)
	if err != nil {
		t.Fatalf("failed to create a test file %s", err.Error())
	}

	srcF = filepath.Base(srcF)
	dstF = filepath.Base(dstF)
	t.Logf("old: %s new: %s", dstD+dstF, dstD+srcF)
	if err = os.Rename(dstD+dstF, dstD+srcF); err != nil {
		t.Fatalf("failed to rename file %s", err.Error())
	}

	viper.Set("source", srcD)
	viper.Set("destination", dstD)

	startLocalSync()
}
*/

func compare(src, dst string) (bool, error) {

	srcF, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer srcF.Close()

	dstF, err := os.Open(dst)
	if err != nil {
		return false, err
	}
	defer dstF.Close()

	h := md5.New()
	_, err = io.Copy(h, srcF)
	if err != nil {
		return false, err
	}

	s1 := h.Sum(nil)
	h.Reset()

	_, err = io.Copy(h, dstF)
	if err != nil {
		return false, err
	}
	s2 := h.Sum(nil)

	if bytes.Equal(s1, s2) {
		return true, nil
	}
	return false, nil
}

func cleanup(files []string) error {
	for _, fn := range files {
		bfn := filepath.Base(fn)
		if err := os.Remove(fn); err != nil {
			return err
		}
		if err := os.Remove(dstD + bfn); err != nil {
			return err
		}
	}
	return nil
}
