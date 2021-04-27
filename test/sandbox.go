package test

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

const blockSize = 8192

func eCheck(err error) {
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
}

func ReadBenchmark() {
	f, err := os.Open("Xync/WinClient.exe")
	eCheck(err)
	fmt.Println("runnign test")
	buf := make([]byte, blockSize)
	start := time.Now()
	for {
		n, err := f.Read(buf)
		if n == 0 {
			if err == io.EOF {
				break
			} else {
				eCheck(err)
			}
			log.Warn().
				Msg("0 bytes read")
		}
	}
	log.Info().
		Dur("unbuff read duration", time.Since(start)).
		Send()
	eCheck(f.Close())

	f, err = os.Open("Xync/WinClient.exe")
	eCheck(err)
	start = time.Now()
	br := bufio.NewReader(f)
	for {
		n, err := br.Read(buf)
		if n == 0 {
			if err == io.EOF {
				break
			} else {
				eCheck(err)
			}
			log.Warn().
				Msg("0 bytes read")
		}
	}
	log.Info().
		Dur("buff read duration", time.Since(start)).
		Send()
	eCheck(f.Close())

	f, err = os.Open("Xync/WinClient.exe")
	eCheck(err)
	start = time.Now()
	br = bufio.NewReader(f)
	for {
		_, err := br.ReadByte()
		if err == io.EOF {
			break
		} else {
			eCheck(err)
		}
	}
	log.Info().
		Dur("buff read byte duration", time.Since(start)).
		Send()
	eCheck(f.Close())

	f, err = os.Open("Xync/WinClient.exe")
	eCheck(err)
	info, err := f.Stat()
	eCheck(err)
	r := io.ReaderAt(f)
	buf = make([]byte, blockSize)
	sr := io.NewSectionReader(r, 0, info.Size())
	start = time.Now()
	for {
		n, err := io.ReadFull(sr, buf)
		if n == 0 {
			if err == io.EOF {
				break
			} else {
				eCheck(err)
			}
		}
	}
	log.Info().
		Dur("unbuff section reader read duration", time.Since(start)).
		Send()
	eCheck(f.Close())

	f, err = os.Open("Xync/WinClient.exe")
	eCheck(err)
	info, err = f.Stat()
	eCheck(err)
	r = io.ReaderAt(f)
	buf = make([]byte, blockSize)
	sr = io.NewSectionReader(r, 0, info.Size())
	br = bufio.NewReader(sr)
	start = time.Now()
	for {
		n, err := io.ReadFull(br, buf)
		if n == 0 {
			if err == io.EOF {
				break
			} else {
				eCheck(err)
			}
		}
	}
	log.Info().
		Dur("buff section reader read duration", time.Since(start)).
		Send()
	eCheck(f.Close())

}
