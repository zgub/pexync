package workers

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

func (hsw *HttpSender) Lock(path string) {
	if l, ok := hsw.eventLocks[path]; ok {
		l.Lock()
	} else {
		var l sync.Mutex
		hsw.eventLocks[path] = &l
		l.Lock()
	}
}

func (hsw *HttpSender) Unlock(path string) error {
	if l, ok := hsw.eventLocks[path]; ok {
		l.Unlock()
	} else {
		return errors.New("invalid path lock")
	}
	return nil
}

func (hsw *HttpSender) StartMon() error {

	log.Info().
		Int("last file index", hsw.lastFileIdx).
		Msg("MONITOR - Starting")

	var err error

	// add new fsnotify watcher
	hsw.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "unable to initialize fs watcher")
	}

	// initialize the watchlist (a map)
	hsw.fileWatchMap = make(map[string]*lfs.FileDesc)
	hsw.eventLocks = make(map[string]*sync.Mutex)

	// add whole source direcotory
	p, err := filepath.Abs(hsw.srcDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine absolute path")
	}
	hsw.watcher.Add(p)

	// add remaining directories
	for _, fd := range hsw.srcList {
		if fd.IsDir == false {
			p := filepath.Join(fd.Prefix, fd.FileName)
			hsw.Watch(p, fd)
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("adding to watchlist")
		}
	}

	spew.Dump(hsw.fileWatchMap)

	for {
		select {
		case event, ok := <-hsw.watcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			// zrusit watchitem a nahradit to lockovanim tu!!!
			hsw.Lock(event.Name)
			err := hsw.eval(event)
			hsw.Unlock(event.Name)
			if err != nil {
				return errors.Wrap(err, "failed parsing fs event")
			}

		case err, ok := <-hsw.watcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		}
	}
}

func (hsw *HttpSender) Watch(path string, fd *lfs.FileDesc) error {
	log.Debug().
		Str("path", path).
		Msg("Monitor - adding to watchlist")
	hsw.fileWatchMapMux.Lock()
	err := hsw.watcher.Add(path)
	if err != nil {
		return err
	}
	hsw.fileWatchMap[path] = fd
	hsw.fileWatchMapMux.Unlock()
	return nil
}

func (hsw *HttpSender) IsWatched(path string) (*lfs.FileDesc, bool) {
	hsw.fileWatchMapMux.Lock()
	fd, ok := hsw.fileWatchMap[path]
	hsw.fileWatchMapMux.Unlock()
	return fd, ok
}

func (hsw *HttpSender) eval(event fsnotify.Event) error {

	/***************
	 * Write event *
	 ***************/
	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("WRITE - event detected")
		// event file descriptor
		efd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrap(err, "file stat error")
		}

		if fd, ok := hsw.IsWatched(event.Name); ok {
			// write event on a known file
			if fd.FileSize == efd.FileSize {
				// size did not change, let's then calculate SHA1 digests
				efd.Sha1, err = efd.GetSha1()
				if err != nil {
					return errors.Wrap(err, "failed to calculate SHA1 digets")
				}
				if bytes.Equal(efd.Sha1, fd.Sha1) {
					// digests are equal, ignore
					log.Info().
						Str("filename", event.Name).
						Msg("WRITE - file has not changed")

					return nil
				} else {
					// digests are not equal - send changes
					log.Info().
						Str("filename", event.Name).
						Msg("WRITE - file content has changed")
				}
			} else {
				// sizes are different - send changes
				log.Debug().
					Str("filename", event.Name).
					Int64("old size", fd.FileSize).
					Int64("new size", efd.FileSize).
					Msg("WRITE - file size changed")
			}

			// to calculate checksum we need to determine the block size first
			if efd.IsDir == false {
				efd.SetBlockSize()
				// beware of empty files
				if efd.BlockSize == 0 {
					efd.BlockSize = 700
				}
			}
			// set the correct file index and state
			efd.State = lfs.Diff
			efd.Idx = fd.Idx
			efd.Sha1 = fd.Sha1

			// first announce the update
			msg := core.NewUPD(hsw.id, efd)
			url := hsw.url.String() + "/meta"

			resp, err := hsw.sendJson(url, msg)
			if err != nil {
				return errors.Wrap(err, "failed to communicate with remote")
			}

			if resp.GetFlag() != core.ACK {
				return errors.New("invalid server response")
			}
			//spew.Dump(efd)

			dstFd := resp.FileDesc
			if dstFd == nil {
				panic("invalid response")
			}

			// send the changes
			if efd.IsDir == false && efd.FileSize != 0 {
				fmt.Println("sending file to roll reader")
				hsw.rrCh <- core.NewRSQ(hsw.id, dstFd, 0, dstFd.FileSize, 1)
				fmt.Printf("%s sent, file size: %d\n", efd.FileName, efd.FileSize)
			}
		} else {
			log.Warn().
				Str("filename", event.Name).
				Msg("WRITE - event on unknown file, ignoring")
			return nil
		}
	}
	/****************
	 * Remove event *
	 ****************/
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("REMOVE - event detected, ignoring")
	}
	/***************
	 * Chmod event *
	 ***************/
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("CHMOD - event detected, ignoring")
	}
	/****************
	 * Cretae event *
	 ****************/
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", event.Name).
			Msg("CREATE - event detected")

		efd, err := lfs.Scan(event.Name)
		if err != nil {
			// let's ignore errors, too may untested edge cases
			return errors.Wrap(err, "file state error")
		}
		//spew.Dump(efd)
		// to calculate checksum we need to determine the block size first

		hsw.lastFileIdx++
		efd.Idx = int64(hsw.lastFileIdx)
		err = hsw.Watch(event.Name, efd)
		if err != nil {
			return errors.Wrap(err, "failed adding file to watchlist")
		}

		if efd.IsDir == false {
			efd.SetBlockSize()
			// beware of empty files
			if efd.BlockSize == 0 {
				efd.BlockSize = 700
			}
			err = core.AddChecksums(efd)
			if err != nil {
				return errors.Wrap(err, "failed adding checksum")
			}
		}

		// first announce the file
		msg := core.NewADD(hsw.id, efd)
		url := hsw.url.String() + "/meta"

		resp, err := hsw.sendJson(url, msg)
		if err != nil {
			log.Fatal().
				Err(err).
				Msg("error comunicating with server")
		}

		if resp.GetFlag() == core.ACK {
			log.Trace().
				Str("filename", event.Name).
				Msg("Monitor - file META sent")
		} else {
			return errors.New("invalid response")
		}

		//spew.Dump(efd)
		// send only if the file is not empty or inf it's not a directory, those have been taken care of already
		// then send the data
		if efd.IsDir == false && efd.FileSize != 0 {
			hsw.brCh <- core.NewRSQ(hsw.id, efd, 0, efd.FileSize, 1)
		}
	}
	/****************
	 * Rename event *
	 ****************/
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("RENAME - event detected")
	}
	return nil
}
