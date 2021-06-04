package workers

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/fsnotify"
	"github.com/zgub/pexync/lfs"
)

type syncState int

const (
	fileSynced syncState = iota
	fileSyncing
	fileWait
)

type fileSync struct {
	status  syncState
	fd      *lfs.FileDesc
	readers int
	mux     sync.Mutex
}

func (hsw *HttpSender) StartMon() error {

	log.Info().
		Int("last file index", hsw.lastFileIdx).
		Msg("MONITOR - Starting")

	var err error

	// add new fsnotify watcher
	hsw.directoryWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "unable to initialize fs watcher")
	}

	// initialize the watchlist (a map)
	hsw.watchedFiles = make(map[string]*lfs.FileDesc)

	// add whole source direcotory
	p, err := filepath.Abs(hsw.srcDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine absolute path")
	}
	// add this one directly, as we don't have a descriptor
	hsw.directoryWatcher.Add(p)

	// add remaining directories
	for _, fd := range hsw.srcList {
		// let's assume nobody ... nah, seems like starting Mon sooner, already when starting the sender
		fd.MonState = lfs.Sent
		if fd.IsDir == false {
			p := filepath.Join(fd.Prefix, fd.FileName)
			hsw.Store(p, fd)
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("adding to watchlist")
		} else {
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("adding to watchlist")
			hsw.directoryWatcher.Add(p)
		}
	}

	//spew.Dump(hsw.fileWatchMap)

	pollInterval := viper.GetInt("poll_interval")

	for {
		select {
		case event, ok := <-hsw.directoryWatcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			fmt.Printf("N E W  *** E V E N T %s for %s\n", event.Op.String(), event.Name)
			err := hsw.evalEvent(event)

			if err != nil {
				return errors.Wrap(err, "failed parsing fs event")
			}

		case err, ok := <-hsw.directoryWatcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		case <-time.After(time.Duration(pollInterval) * time.Second):
			log.Trace().
				Msg("Monitor polling changes")
		}
	}
}

// Store stores the file descriptor in a shared map of monitored files
func (hsw *HttpSender) Store(path string, fd *lfs.FileDesc) error {
	// watch map is shared, Lock for write
	hsw.mux.Lock()
	defer hsw.mux.Unlock()

	log.Debug().
		Str("path", path).
		Msg("Monitor - adding file to list of known files")
	if fd.IsDir {
		err := hsw.directoryWatcher.Add(path)
		if err != nil {
			return err
		}
		log.Debug().
			Str("filename", fd.FileName).
			Msg("Monitor - adding dir to watcher")
	}
	// rewrite or add, does not matter
	hsw.watchedFiles[path] = fd
	// watch map is shared, Unlock
	return nil
}

// IsKnown is similar to sync.Map Load, that is returns map value for a given key if it exists
// false as the second result otherwise
func (hsw *HttpSender) Load(path string) (fd *lfs.FileDesc, ok bool) {
	// watch map is shared, Lock for read, map is not thread safe
	hsw.mux.RLock()
	defer hsw.mux.RUnlock()
	fd, ok = hsw.watchedFiles[path]
	return
}

// SetState sets a state of the file descriptor or returns false if the key does not exist
func (hsw *HttpSender) SetState(path string, state lfs.MonitorState) (ok bool) {
	hsw.mux.Lock()
	defer hsw.mux.Unlock()
	if fd, ok := hsw.watchedFiles[path]; ok {
		fd.MonState = state
	}
	return
}

func (hsw *HttpSender) evalEvent(event fsnotify.Event) error {

	/****************
	 * Create event *
	 ****************/
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL CREATE")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Created
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
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
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Changed
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}

	}
	/********************
	 * Write event *
	 ********************/
	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL WRITE - ignoring")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Created
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}
	}
	/****************
	 * Remove event *
	 ****************/
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL REMOVE - ignoring")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Deleted
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}
	}
	/***************
	 * Chmod event *
	 ***************/
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL CHMOD - ignoring")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Metadata
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}
	}
	/****************
	 * Rename event *
	 ****************/
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("EVAL RENAME - TODO")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		}
		fd.MonState = lfs.Renamed
		err = hsw.Store(event.Name, fd)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}

	}
	return nil
}
