package workers

import (
	"fmt"
	"path/filepath"

	//"github.com/fsnotify/fsnotify"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/fsnotify"
	"github.com/zgub/pexync/lfs"
)

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

	// add whole source direcotory
	p, err := filepath.Abs(hsw.srcDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine absolute path")
	}
	// add this one directly, as we don't have a descriptor
	hsw.watcher.Add(p)

	// add remaining directories
	for _, fd := range hsw.srcList {
		if fd.IsDir == false {
			p := filepath.Join(fd.Prefix, fd.FileName)
			hsw.AddToMonList(p, fd)
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("adding to watchlist")
		}
	}

	//spew.Dump(hsw.fileWatchMap)

	for {
		select {
		case event, ok := <-hsw.watcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			fmt.Printf("N E W  *** E V E N T %s for %s\n", event.Op.String(), event.Name)
			err := hsw.evalEvent(event)

			if err != nil {
				return errors.Wrap(err, "failed parsing fs event")
			}

		case err, ok := <-hsw.watcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
			case 
		}
	}
}

func (hsw *HttpSender) AddToMonList(path string, fd *lfs.FileDesc) error {
	log.Debug().
		Str("path", path).
		Msg("Monitor - adding file to list of known files")
	//spew.Dump(fd)
	hsw.fileWatchMapMux.Lock()
	if fd.IsDir {
		err := hsw.watcher.Add(path)
		if err != nil {
			return err
		}
		log.Debug().
			Str("filename", fd.FileName).
			Msg("Monitor - adding dir to watcher")
	}
	hsw.fileWatchMap[path] = fd
	hsw.fileWatchMapMux.Unlock()
	return nil
}

func (hsw *HttpSender) IsKnown(path string) (*lfs.FileDesc, bool) {
	hsw.fileWatchMapMux.Lock()
	fd, ok := hsw.fileWatchMap[path]
	hsw.fileWatchMapMux.Unlock()
	return fd, ok
}

func (hsw *HttpSender) evalEvent(event fsnotify.Event) error {

	/****************
	 * Create event *
	 ****************/
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL CREATE")

		// TEST
		//return nil

		efd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrap(err, "file state error")
		}

		//spew.Dump(efd)
		// to calculate checksum we need to determine the block size first

		hsw.lastFileIdx++
		efd.Idx = int64(hsw.lastFileIdx)
		efd.State = lfs.Missing
		// adding to know files
		err = hsw.AddToMonList(event.Name, efd)
		if err != nil {
			return errors.Wrap(err, "failed adding file to watchlist")
		}

		// first announce the files
		log.Trace().
			Str("filename", efd.FileName).
			Int64("file index", efd.Idx).
			Msg("CREATE**************** sending meta")
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
				Msg("Monitor - CREATE new-file META sent")
		} else {
			return errors.New("invalid response")
		}

	}
	/**********************
	 * Close  Write event *
	 **********************/
	if event.Op&fsnotify.CloseWrite == fsnotify.CloseWrite {
		//
		log.Info().
			Str("path", event.Name).
			Msg("EVAL CLOSE WRITE")

		// event file descriptor
		efd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrap(err, "file stat error")
		}
		//hsw.FileUnlock(event.Name)

		if fd, ok := hsw.IsKnown(event.Name); ok {
			fmt.Println("old")
			spew.Dump(fd)
			if fd.FileSize == efd.FileSize {
				fmt.Printf("EVAL CLOSE WRITE %s ****************** same size\n", event.Name)
			} else {
				fmt.Printf("EVAL CLOSE WRITE %s ****************** different size\n", event.Name)
				efd.Idx = fd.Idx
				// first announce the update
				fmt.Println("sending")
				spew.Dump(efd)
				msg := core.NewUPD(hsw.id, efd)
				url := hsw.url.String() + "/meta"

				resp, err := hsw.sendJson(url, msg)
				if err != nil {
					return errors.Wrap(err, "failed to communicate with remote")
				}

				if resp.GetFlag() != core.ACK {
					return errors.New("invalid server response")
				}

				fmt.Println("received")
				dstFd := resp.GetFileDesc()
				spew.Dump(dstFd)
			}
			// update the cached state
			fd = efd
			//spew.Dump(fd)
		} else {
			panic("unknown unmonitored file")
		}

		//hsw.FileUnlock(event.Name)

		/*
			if fd, ok := hsw.IsKnown(event.Name); ok {
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
							Msg("CLOSE WRITE - file has not changed")

						// unlock!!!
						fLock.Unlock()
						return nil
					} else {
						// digests are not equal - send changes
						log.Info().
							Str("filename", event.Name).
							Msg("CLOSE WRITE - file content has changed")
					}
				} else {
					// sizes are different - send changes
					log.Debug().
						Str("filename", event.Name).
						Int64("old size", fd.FileSize).
						Int64("new size", efd.FileSize).
						Msg("CLOSE WRITE - file size changed")
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
					hsw.rrCh <- core.NewAsyncRSQ(hsw.id, dstFd, 0, dstFd.FileSize, 1, fLock)
				}
			} else {
				log.Warn().
					Str("filename", event.Name).
					Msg("CLOSE WRITE - event on unknown file, ignoring")
				return nil
			}
		*/
	}
	/********************
	 * Write event *
	 ********************/
	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL WRITE - ignoring")
	}
	/****************
	 * Remove event *
	 ****************/
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL REMOVE - ignoring")
	}
	/***************
	 * Chmod event *
	 ***************/
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL CHMOD - ignoring")
	}
	/****************
	 * Rename event *
	 ****************/
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL RENAME - TODO")
	}
	return nil
}
