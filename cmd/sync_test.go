package cmd

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

const (
	srcD = "../test/"
	dstD = "../Xync/"
)

func (t testFileType) String() string {
	return testFileTypes[t]
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
	if err = cleanup(testFiles...); err != nil {
		t.Errorf("unable to remove test files")
	}

}

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

	eq, err := compare(srcD+srcF, dstD+srcF)
	if err != nil {
		t.Fatalf("failed to compare files: %s", err.Error())
	}
	if !eq {
		t.Fatalf("\n *** %s \n *** %s \n *** not equal", srcD+srcF, dstD+srcF)
	}
	err = cleanup(srcD+srcF, dstD+srcF)
}

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

func cleanup(files ...string) error {
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
