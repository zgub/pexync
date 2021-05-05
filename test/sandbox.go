package test

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/adler32"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
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

func RollV3(srcS, dstS string) {

	var out []byte

	sr := bytes.NewReader([]byte(srcS))
	dr := bytes.NewReader([]byte(dstS))
	bs := int64(3)
	rh := core.Pour(bs)
	a32 := adler32.New()
	hm := make(map[uint32][]byte)

	for {
		buf := make([]byte, bs)
		n, err := io.ReadFull(dr, buf)
		if n == 0 {
			if err == io.EOF {
				break
			} else {
				log.Fatal().
					Err(err).
					Msg("failed to read data")
			}
		}
		buf = buf[:n]
		a32.Write(buf)
		aSum := a32.Sum32()
		hm[aSum] = buf
		a32.Reset()
	}
	for k, v := range hm {
		fmt.Printf("%d -> %s\n", k, string(v))
	}

	n, err := io.CopyN(rh, sr, bs)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to read data")
	}
	fmt.Printf("initial read %d bytes into hash: %s\n", n, string(rh.GetWindow()))

	// this works even if the read is not full, as the buffer is new = empty
	buf := new(bytes.Buffer)
	n, err = io.CopyN(buf, sr, bs)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to read data")
	}
	fmt.Printf("initial read %d bytes into buffer: %s\n", n, string(buf.Bytes()))

	var idx, bts int

	for done := false; !done; {
		rSum := rh.Sum32()
		//fmt.Printf("window len: %d\n", len(rh.GetWindow()))
		if len(rh.GetWindow()) != 3 {
			log.Error().
				Msg("invalid window size")
		}

		/*
			fmt.Printf("??? %s (%d)\n", rh.GetWindow(), rSum)
			for k, v := range hm {
				fmt.Printf("??? %d -> %s\n", k, string(v))
			}
		*/

		if hIndex, ok := hm[rSum]; ok {

			/*********
			 * MATCH *
			 *********/

			//fmt.Println("*** MATCH ***")

			//fmt.Printf("index -> %s\n", string(hIndex))
			idx++
			out = append(out, hIndex...)
			//fmt.Printf("out: %s\n", string(out))

			rh.Reset()
			//n, err = io.CopyN(rh, buf, bs)
			// can't use io.Copy, we need to initialize the hash window with correct block size
			if int64(buf.Len()) < bs {
				n, err = io.CopyN(buf, sr, bs-int64(buf.Len()))
				if n == 0 {
					if err == io.EOF {
						// 0 loaded, break
						break
					} else {
						log.Fatal().
							Err(err).
							Caller().
							Msg("error reading data")
					}
				}
			}
			m, err := rh.Write(buf.Bytes())
			if m == 0 {
				if err == io.EOF {
					// 0 loaded, break
					break
				} else {
					log.Fatal().
						Err(err).
						Caller().
						Msg("error reading data")
				}
			}
			//fmt.Printf("read new %d bytes from buffer into hash: %s\n", n, string(rh.GetWindow()))
			buf.Reset()
			n, err = io.CopyN(buf, sr, bs)
			if n == 0 {
				if err == io.EOF {
					// 0 bytes loaded, break
					break
				} else {
					log.Fatal().
						Err(err).
						Msg("error reading data")
				}
			}
			//fmt.Printf("read new %d bytes into buffer: %s\n", n, string(buf.Bytes()))
			continue
		} else {

			/************
			 * NO MATCH *
			 ************/

			//fmt.Println("*** NO MATCH ***")
			// we have some new data in buffer and some old in hash
			if buf.Len() == 0 {
				n, err = io.CopyN(buf, sr, bs)
				//fmt.Printf("===============>  empty buffer, loaded %d bytes: %s\n", n, string(buf.Bytes()))
				if n == 0 {
					if err == io.EOF {
						// loaded 0 bytes, no more data, break
						//fmt.Println("===> roller out of bytes")
						break
					} else {
						log.Fatal().
							Err(err).
							Msg("error reading data")
					}
				}
			}
			nb, err := buf.ReadByte()
			if err != nil {
				log.Fatal().
					Err(err).
					Msg("error reading data")
			}
			ob := rh.Roll(nb)
			//fmt.Printf("%s\n", srcS)
			//fmt.Printf("loaded new byte: %s into hash, emitted old one: %s\n", string(nb), string(ob))
			out = append(out, ob)
			//fmt.Printf("added: %s to %s\n", string(ob), string(out))
			bts++
		}
	}
	w := rh.GetWindow()
	out = append(out, w...)
	fmt.Printf("src:\t%s\n", srcS)
	fmt.Printf("out:\t%s\n", out)
	fmt.Printf("dst:\t%s\n", dstS)
	fmt.Printf("index hits: %d, uniques bytes: %d\n", idx, bts)
}
