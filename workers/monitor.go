package workers

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

func (hs HttpSender) StartMon() error {

	log.Info().
		Int("last file index", hs.idx).
		Msg("MONITOR - Starting")

	var err error

	// add new fsnotify watcher
	hs.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "unable to initialize fs watcher")
	}

	// initialize the watchlist (a map)
	hs.watchMap = make(map[string]*lfs.FileDesc)

	// add whole source direcotory
	p, err := filepath.Abs(hs.srcDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine absolute path")
	}
	hs.watcher.Add(p)

	// add remaining directories
	for _, fd := range hs.srcList {
		if fd.IsDir {
			p := filepath.Join(fd.Prefix, fd.FileName)
			hs.watchMap[p] = fd
			hs.watcher.Add(p)
		}
	}

	for {
		select {
		case event, ok := <-hs.watcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			hs.eval(event)

		case err, ok := <-hs.watcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		}
	}
}

func (hs HttpSender) Watch(path string) error {
	log.Debug().
		Str("path", path).
		Msg("Monitor - adding to watchlist")
	return hs.watcher.Add(path)
}

func (hs HttpSender) eval(event fsnotify.Event) {

	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("WRITE - event detected")
		// event file descriptor
		efd, err := lfs.Scan(event.Name)
		if err != nil {
			log.Error().
				Err(err).
				Msg("file stat error")
			// let's ignore errors, too may untested edge cases
			return
		}
		if fd, ok := hs.watchMap[event.Name]; ok {
			// write event on a known file
			if fd.FileSize == efd.FileSize {
				// size did not change, let's then calculate SHA1 digests
				efd.Sha1, err = efd.GetSha1()
				if err != nil {
					log.Error().
						Err(err).
						Str("filename", event.Name).
						Msg("failed to calulate SHA1 digest")
					return
				}
				if bytes.Equal(efd.Sha1, fd.Sha1) {
					// digests are equal, ignore
					log.Info().
						Str("filename", event.Name).
						Msg("WRITE - file has not changed")
					return
				} else {
					// digests are not equal - send changes
					log.Info().
						Str("filename", event.Name).
						Msg("WRITE - file has changed")
					hs.watchMap[event.Name] = efd
				}
			} else {
				// sizes are different - send changes
				log.Info().
					Str("filename", event.Name).
					Msg("WRITE - file size change")
			}
		} else {
			log.Warn().
				Str("filename", event.Name).
				Msg("WRITE - event on unknown file, ignoring")
			return
		}
	}
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("REMOVE - event detected, ignoring")
	}
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("CHMOD - event detected, ignoring")
	}
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", event.Name).
			Msg("CREATE - event detected")

		efd, err := lfs.Scan(event.Name)
		if err != nil {
			log.Error().
				Err(err).
				Msg("file stat error")
			// let's ignore errors, too may untested edge cases
			return
		}
		//spew.Dump(efd)
		// to calculate checksum we need to determine the block size first

		if !efd.IsDir {
			efd.SetBlockSize()
			// beware of empty files
			if efd.BlockSize == 0 {
				efd.BlockSize = 700
			}
			err = core.AddChecksums(efd)
			if err != nil {
				log.Error().
					Err(err).
					Msg("Monitor event - failed to calculate checksums")
				return
			}
		}

		hs.idx++
		efd.Idx = int64(hs.idx)
		hs.watchMap[event.Name] = efd

		// first announce the file
		fdList := []*lfs.FileDesc{efd}
		msg := core.NewADD(hs.id, fdList)

		// send
		url := hs.url.String() + "/meta"
		resp, err := hs.sendJson(url, msg)
		if err != nil {
			log.Fatal().
				Err(err).
				Msg("error comunicating with server")
		}

		if resp.GetFlag() == core.ACK {
			log.Trace().
				Str("filename", event.Name).
				Msg("Monitor - file sent")
			hs.idx++
		} else {
			log.Error().
				Msg("error sending file")
		}

		//spew.Dump(resp)

		// then send the data
		fmt.Println("sending")
		hs.brCh <- core.NewRSQ(hs.id, efd, 0, efd.FileSize, 1)
	}
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("RENAME - event detected")
	}

}
