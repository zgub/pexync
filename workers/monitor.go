package workers

import (
	"fmt"
	"path/filepath"
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
	fileWrite
	fileMeta
	fileRemoved
	fileRenamed
)

// fileSync holds the sync state of a file, including the number of running readrs
type fileSync struct {
	status  syncState
	fd      *lfs.FileDesc
	readers int
}

func newFileSync(fd *lfs.FileDesc) *fileSync {
	return &fileSync{
		status: fileWrite,
		fd:     fd,
	}
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
	hsw.syncStatus = make(map[string]*fileSync)

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
		if fd.IsDir == false {
			p := filepath.Join(fd.Prefix, fd.FileName)
			hsw.updateSyncStatus(p, fd, fileSynced)
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
	ccIo := viper.GetInt("io_concurrency")

	checkSyncStatus := func() error {

		// acquire a read lock
		hsw.syncStatusMux.RLock()
		defer hsw.syncStatusMux.Unlock()
		syncCount := 0
		// determine the count of working reader goroutines
		for _, s := range hsw.syncStatus {
			if s.status == fileSyncing {
				syncCount++
			}
		}

		// if there are free goroutines, send data
		if syncCount < ccIo {
			/**************************
			 ** TODO                  *
			 ** determine change type *
			 ** start the readers     *
			 ** send data             *
			 **************************/
		}

		if syncCount > ccIo {
			return errors.New("too many sync processes running")
		}

		return nil
	}

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

			err = checkSyncStatus()
			if err != nil {
				return errors.Wrap(err, "monitor - sync check failed")
			}

		case err, ok := <-hsw.directoryWatcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		case <-time.After(time.Duration(pollInterval) * time.Second):
			log.Trace().
				Msg("Monitor - sync state check")
			err = checkSyncStatus()
			if err != nil {
				return errors.Wrap(err, "monitor - sync check failed")
			}
		}
	}
}

// updateSyncStatus updates the status in a shared map making sure that no race condition occurs
func (hsw *HttpSender) updateSyncStatus(path string, fd *lfs.FileDesc, status syncState) error {
	// watch map is shared, Lock for write
	hsw.syncStatusMux.Lock()
	defer hsw.syncStatusMux.Unlock()

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

	if s, ok := hsw.syncStatus[path]; ok {
		// file was already monitored
		s.status = status
	} else {
		s := newFileSync(fd)
		s.status = status
		hsw.syncStatus[path] = s
	}

	return nil
}

// getSyncStatus returns a status of monitored file, to check whether it has been changed while syncing
func (hsw *HttpSender) getSyncStatus(path string) syncState {
	hsw.syncStatusMux.RLock()
	defer hsw.syncStatusMux.Unlock()
	if s, ok := hsw.syncStatus[path]; ok {
		return s.status
	}
	panic("unmonitored path")
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
		err = hsw.updateSyncStatus(event.Name, fd, fileWrite)
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
		err = hsw.updateSyncStatus(event.Name, fd, fileWrite)
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
		//fd, err := lfs.Scan(event.Name)
		//if err != nil {
		//	return errors.Wrapf(err, "failed to stat new file %s", event.Name)
		//}
		//err = hsw.updateSyncStatus(event.Name, fd, fileWrite)
		//if err != nil {
		//	return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		//}
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
		err = hsw.updateSyncStatus(event.Name, fd, fileRemoved)
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
		err = hsw.updateSyncStatus(event.Name, fd, fileMeta)
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
		err = hsw.updateSyncStatus(event.Name, fd, fileRenamed)
		if err != nil {
			return errors.Wrapf(err, "unable to monitor file %s", event.Name)
		}

	}
	return nil
}
