package workers

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"
	"unicode/utf8"
)

func createTestFile(blockSize, count int) error {
	path := fmt.Sprintf("test/%dx%d-test-data", count, blockSize)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(io.Writer(f))
	rn := rune('a')
	buf := make([]byte, utf8.UTFMax)
	for block := 0; block < count; block++ {
		for byte := 0; byte < blockSize; byte++ {
			n := utf8.EncodeRune(buf, rn)
			buf = buf[:n]
			n, err = bw.Write(buf)
			if err != nil {
				return err
			}
		}
		rn++
	}

	bw.Flush()
	f.Sync()
	return nil
}

func TestMissingLocalSync(t *testing.T) {

}
