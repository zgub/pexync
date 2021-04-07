package core

import (
	"bufio"
	"fmt"
	"hash/adler32"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func TestSectionReader(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	r := io.ReaderAt(f)
	sectionSize := int64(1048576)
	var (
		wg sync.WaitGroup
	)
	const (
		blockSize = 1024
		offset    = 512
	)
	wg.Add(10)
	var pos int64 = 0
	for gr := 0; gr < 10; gr++ {
		// pos = 0; pos < 10; pos++ {}
		sr := io.NewSectionReader(r, pos, sectionSize)

		if gr%2 == 0 {

			go func(id int, startAt int64) {
				//br := bufio.NewReader(sr)
				defer wg.Done()
				s, err := sr.Seek(offset, io.SeekCurrent)
				if err != nil {
					log.Error().
						Err(err).
						Int("id", id).
						Msg("UNBUF")
					return
				}
				log.Info().
					Int64("offset", s).
					Msg("seek")

				buf := make([]byte, blockSize)
				start := time.Now()
				var i = 0
				for ; ; i++ {

					n, err := io.ReadFull(sr, buf)
					if n == 0 {
						/*
							if err == nil {
								continue
							}
						*/

						if err == nil {
							log.Info().
								Msg("read zero bytes")
							continue
						} else if err != io.EOF {
							log.Error().
								Err(err).
								Int("id", id).
								Msg("UNBUF")
						}
						if err == io.EOF {
							break
						}

					}
				}
				log.Info().
					Int("UNBUF ID", id).
					TimeDiff("duration", time.Now(), start).
					Int("cycles", i).
					Int64("offset", startAt).
					Int64("section size", sectionSize).
					Int64("block size", blockSize).
					Float64("speed", (float64(sectionSize)/(float64(time.Since(start).Seconds())))/1024).
					Msg("done")
			}(gr, pos)
		} else {
			go func(id int, startAt int64) {

				defer wg.Done()
				s, err := sr.Seek(offset, io.SeekCurrent)
				if err != nil {
					log.Error().
						Err(err).
						Int("id", gr).
						Msg("BUF")
					return
				}
				log.Info().
					Int64("offset", s).
					Msg("seek")

				buf := make([]byte, blockSize)
				start := time.Now()
				var i = 0
				br := bufio.NewReader(sr)
				for ; ; i++ {
					n, err := io.ReadFull(br, buf)
					if n == 0 {

						if err == nil {
							log.Info().
								Msg("read zero bytes")
							continue
						} else if err != io.EOF {
							log.Error().
								Err(err).
								Int("id", id).
								Msg("BUF")
						}
						if err == io.EOF {
							break
						}
					}
				}
				log.Info().
					Int("BUF ID", id).
					TimeDiff("duration", time.Now(), start).
					Int("cycles", i).
					Int64("offset", startAt).
					Int64("section size", sectionSize).
					Int64("block size", blockSize).
					Float64("speed", (float64(sectionSize)/(float64(time.Since(start).Seconds())))/1024).
					Msg("done")
			}(gr, pos)
		}
		pos += sectionSize
	}
	wg.Wait()
	return nil
}

func TestSectionSum(fileName string) error {
	log.Info().
		Msg("start")
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	blockSize := viper.GetInt("block_size")

	r := io.ReaderAt(f)
	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size()
	blockCount := size / int64(blockSize)
	if (size % int64(blockCount)) != 0 {
		blockCount++
	}

	buffer := make([]byte, blockSize)
	var hashList []uint32
	sectionSize := size / 2
	var pos int64
	sr := io.NewSectionReader(r, pos, sectionSize)
	br := bufio.NewReader(sr)
	rh := Pour()
	n, err := io.ReadFull(br, buffer)
	if n == 0 || err != nil {
		return err
	}
	rh.Write(buffer)
	hashList = append(hashList, rh.Sum32())
	var x, y int
	log.Info().
		Msg("first section")
	for ; ; x++ {
		n, err := io.ReadFull(br, buffer)
		if n == 0 {
			if err == nil {
				// read 0 bytes but no err, funny stuff
				log.Info().
					Msg("read zero bytes")
				continue
			} else if err != io.EOF {
				// read zero bytes with an error different that EOF
				log.Error().
					Err(err).
					Msg("error")
			}
			if err == io.EOF {
				// well EOF
				break
			}
		}
		for _, b := range buffer {
			rh.Roll(b)
			hashList = append(hashList, rh.Sum32())
		}
	}
	log.Info().
		Msg("first section end")
	rh.Reset()
	sr = io.NewSectionReader(r, sectionSize-int64(blockSize), size)
	br = bufio.NewReader(sr)
	n, err = io.ReadFull(br, buffer)
	if n == 0 || err != nil {
		return err
	}
	rh.Write(buffer)
	hashList = append(hashList, rh.Sum32())
	log.Info().
		Msg("second section")
	for ; ; y++ {
		n, err := io.ReadFull(br, buffer)
		if n == 0 {
			if err == nil {
				// read 0 bytes but no err, funny stuff
				log.Info().
					Msg("read zero bytes")
				continue
			} else if err != io.EOF {
				// read zero bytes with an error different that EOF
				log.Error().
					Err(err).
					Msg("error")
			}
			if err == io.EOF {
				// well EOF
				break
			}
		}
		for _, b := range buffer {
			rh.Roll(b)
			hashList = append(hashList, rh.Sum32())
		}
	}
	log.Info().
		Int64("size", size).
		Int64("block count", blockCount).
		Float64("sqrt(size)", math.Sqrt(float64(size))).
		Int("sr1 block number", x).
		Int("sr2 block number", y).
		Int("sr1 + sr2", x+y).
		Int("hash list len", len(hashList)).
		Msg("stat")

	nr := io.Reader(f)
	rh.Reset()
	buffer = make([]byte, blockSize)
	br = bufio.NewReader(nr)
	n, err = io.ReadFull(br, buffer)
	if n == 0 || err != nil {
		return err
	}
	rh.Write(buffer)
	if rh.Sum32() != hashList[0] {
		log.Fatal().
			Msg("missmatch")
	}
	log.Info().
		Msg("check")
	for z := 1; ; z++ {
		n, err := io.ReadFull(br, buffer)
		if n == 0 {
			if err == nil {
				// read 0 bytes but no err, funny stuff
				log.Info().
					Msg("read zero bytes")
				continue
			} else if err != io.EOF {
				// read zero bytes with an error different that EOF
				log.Error().
					Err(err).
					Msg("error")
			}
			if err == io.EOF {
				// well EOF
				break
			}
			for _, b := range buffer {
				rh.Roll(b)
				if rh.Sum32() != hashList[z] {
					log.Error().
						Err(err).
						Msg("error")
				}

			}
		}
	}
	return nil
}

type testBuffer struct {
	cap  int
	pos  int
	data []byte
}

func NewTestBuffer(capacity int) *testBuffer {
	return &testBuffer{
		cap:  capacity,
		pos:  0,
		data: make([]byte, capacity),
	}
}

func (b *testBuffer) reset() {
	b.pos = 0
}

func (b *testBuffer) get() []byte {
	return b.data[:b.pos]
}

func (b *testBuffer) push(bt byte) {
	b.data[b.pos] = bt
	b.pos++
}

func RunBufferTest() {
	f, err := os.Open("test/testfile")
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	readBuff := make([]byte, 3000)
	log.Info().Msg("starting realloc test")
	start := time.Now()
	num := int64(0)
	for {
		r := io.Reader(f)
		buf := make([]byte, 3000)
		n, err := io.ReadFull(r, readBuff)
		if n == 0 {

			if err == nil {
				log.Info().
					Msg("read zero bytes")
					// well, that's cute, let's try again
				continue
			} else if err != io.EOF {
				log.Fatal().
					Caller().
					Stack().
					Err(err).
					Send()
			}
			if err == io.EOF {
				// yay, nd of file, ehm section, well this should be addresses
				break
			}
		}
		for i, b := range readBuff {
			buf[i] = b
		}
		num += int64(n)
	}
	f.Close()
	log.Info().TimeDiff("duration", time.Now(), start).Int64("bytes read", num).Msg("realloc test")

	f, err = os.Open("test/testfile")
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	defer f.Close()

	log.Info().Msg("starting buffer test")
	start = time.Now()
	num = int64(0)
	buf := NewTestBuffer(3000)
	for {
		r := io.Reader(f)
		buf.reset()
		n, err := io.ReadFull(r, buf.data)
		if n == 0 {

			if err == nil {
				log.Info().
					Msg("read zero bytes")
					// well, that's cute, let's try again
				continue
			} else if err != io.EOF {
				log.Fatal().
					Caller().
					Stack().
					Err(err).
					Send()
			}
			if err == io.EOF {
				// yay, nd of file, ehm section, well this should be addresses
				break
			}
		}
		for _, b := range readBuff {
			buf.push(b)
		}
		num += int64(n)
	}
	log.Info().TimeDiff("duration", time.Now(), start).Int64("bytes read", num).Msg("buf test")

}

func SeekTest() {
	log.Info().Msg("seek test start")
	f, err := os.Open("test/seekTestFile")
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	blockSize := 4
	r := io.ReaderAt(f)
	sr := io.NewSectionReader(r, 0, info.Size())
	buf := make([]byte, blockSize)
	n, err := io.ReadFull(sr, buf)
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	log.Info().
		Int("bytes read", n).
		Str("bytes", string(buf)).
		Send()
		/*
					pos, err := sr.Seek(int64(blockSize), io.SeekCurrent)
					if err != nil {
						log.Fatal().
							Caller().
							Stack().
							Err(err).
							Send()
					}

			log.Info().
				Int64("seek position", pos).
				Send()
		*/
	n, err = io.ReadFull(sr, buf)
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	log.Info().
		Int("bytes read", n).
		Str("bytes", string(buf)).
		Send()
}

func RollTest() {
	log.Info().Msg("seek test start")
	f, err := os.Open("test/seekTestFile")
	if err != nil {
		log.Fatal().
			Caller().
			Stack().
			Err(err).
			Send()
	}
	defer f.Close()

	blockSize := 4
	buf := make([]byte, blockSize)
	r := io.Reader(f)
	for {
		n, err := io.ReadFull(r, buf)
		if n == 0 {

			if err == nil {
				log.Info().
					Msg("read zero bytes")
					// well, that's cute, let's try again
				continue
			} else if err != io.EOF {
				log.Fatal().
					Caller().
					Stack().
					Err(err).
					Send()
			}
			if err == io.EOF {
				// yay, nd of file, ehm section, well this should be addresses
				break
			}
		}
		sum := adler32.Checksum(buf)
		fmt.Printf("data: %s\t sum: %d", string(buf), sum)
	}
}
