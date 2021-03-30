package core

import (
	"bufio"
	"io"
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
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	blockSize := viper.GetInt("block_size")

	//r := io.ReaderAt(f)
	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}
	size := fileInfo.Size()
	blockCount := size / int64(blockSize)
	if (size % int64(blockCount)) != 0 {
		blockCount++
	}

	//	buffer := make([]byte, blockSize)
	//	hashList := make([]uint32, blockCount)
	log.Info().
		Int64("size", size).
		Int64("block count", blockCount).
		Msg("stat")
	return nil
}
