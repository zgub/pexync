package workers

import (
	"path/filepath"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/fsnotify"
	"github.com/zgub/pexync/lfs"
	"golang.org/x/sync/errgroup"
)

// StartMon start the sender in monitoring mode
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
	hsw.syncStatus = make(map[string]*lfs.FileDesc)

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
			// sure?
			fd.SetState(lfs.Synced)
			hsw.syncStatus[p] = fd
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("monitor - file added to watchlist")
		} else {
			// watch this directory as well
			hsw.directoryWatcher.Add(p)
			// add to watch list
			hsw.syncStatus[p] = fd
			log.Trace().
				Str("filename", fd.FileName).
				Int64("filesize", fd.FileSize).
				Msg("directory added to watchlist")

		}
	}

	ccIo := viper.GetInt("io_concurrency")
	readersErrGroup := new(errgroup.Group)
	// start te readers
	for i := 0; i < ccIo; i++ {
		fr := NewFileReader(hsw.ctx, hsw.rrCh, hsw.receiver)
		log.Trace().
			Msgf("monitor - starting file reader: %d", i)
		readersErrGroup.Go(fr.Run)
	}

	pollInterval := viper.GetDuration("poll_interval")

	for {
		select {
		case <-hsw.ctx.Done():
			log.Debug().
				Msg("Monitor - recived cancel, exiting")
			return nil
		case event, ok := <-hsw.directoryWatcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			log.Trace().
				Str("filename", event.Name).
				Str("operation", event.String()).
				Str("operation", event.Op.String()).
				Msg("new fs event")

			err := hsw.evalEvent(event)

			if err != nil {
				return errors.Wrap(err, "failed parsing fs event")
			}

			err = hsw.checkpoint()
			if err != nil {
				return errors.Wrap(err, "monitor - sync check failed")
			}

		case err, ok := <-hsw.directoryWatcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		case <-time.After(pollInterval * time.Second):
			log.Trace().
				Msg("monitor - checkpoint")
			err = hsw.checkpoint()
			if err != nil {
				return errors.Wrap(err, "monitor - sync check failed")
			}
		}
	}
}

// checkpoint sends the data if free readrs are available and files were change
func (hsw *HttpSender) checkpoint() error {

	// ccIo = max nibmber of readers possible
	ccIo := viper.GetInt("io_concurrency")
	busyReaders := 0
	toBeSynced := make([]*lfs.FileDesc, 0)

	// first ceck whether there are free readers
	// and colled files that need to be synced in the same run
	for _, fd := range hsw.syncStatus {
		if fd.GetState() == lfs.InSync {
			busyReaders++
		}
		// if it reaches ccIo return, no free readeers
		if busyReaders == ccIo {
			// this is not optimal, we could at least send the meta
			log.Debug().
				Msg("monitor checkpoint - no free readers")
			return nil
		}

		// collect modified items
		if fd.GetState() != lfs.InSync && fd.GetState() != lfs.Synced {
			toBeSynced = append(toBeSynced, fd)
		}
	}

	freeReaders := ccIo - busyReaders

	log.Debug().
		Msgf("monitor checkpoint - %d free readers", freeReaders)

	// here we should have at least one free readers
	// check if there any files to be sent, quit otherwise
	if len(toBeSynced) == 0 {
		// no files to sync
		return nil
	}

	log.Debug().
		Msgf("monitor checkpoint - %d files to sync", len(toBeSynced))

	// we need to proceed in order because directories have to be created first
	// maps are not sorted
	// sorting things out
	sort.Slice(toBeSynced, func(i, j int) bool {
		return toBeSynced[i].Modified.Before(toBeSynced[j].Modified)
	})

	// we have free readers and files to sync, let's go file by file
	url := hsw.url.String() + "/meta"
	for _, fd := range toBeSynced {
		switch fd.GetState() {
		case lfs.Created:
			// sending new file, this will pnly update the src list on the remote
			msg := core.NewADD(hsw.id, fd)
			log.Trace().
				Msg("sending new file message")
			_, err := hsw.sendJson(url, msg)
			if err != nil {
				return errors.Wrap(err, "failed to send medadata to htp server")
			}
			// change the creatd state to missing state, recever was notified
			fd.SetState(lfs.Missing)
		case lfs.Missing:
			// sending whole file
			if freeReaders > 0 {
				hsw.rrCh <- core.NewRSQ(hsw.id, fd, 0, fd.FileSize, 1)
				freeReaders--
			} else {
				// no more readers
				return nil
			}
		case lfs.Diff:
			// sending delta, or new file, the bussiness
			if freeReaders > 0 {
				hsw.rrCh <- core.NewRSQ(hsw.id, fd, 0, fd.FileSize, 1)
				freeReaders--
			} else {
				// no more readers
				return nil
			}
		case lfs.Meta:
			// sending only meta
			msg := core.NewMOD(hsw.id, fd)
			log.Trace().
				Msg("sending chmod message")
			_, err := hsw.sendJson(url, msg)
			if err != nil {
				return errors.Wrap(err, "failed to send medadata to htp server")
			}
			// this was only meta, let's continue
		case lfs.Renamed:
			// sending only meta
			msg := core.NewREN(hsw.id, fd)
			log.Trace().
				Msg("sending rename message")
			_, err := hsw.sendJson(url, msg)
			if err != nil {
				return errors.Wrap(err, "failed to send medadata to htp server")
			}
			// this was only meta, let's continue
		case lfs.Deleted:
			// seding only meta
			msg := core.NewDEL(hsw.id, fd)
			log.Trace().
				Msg("sending delete message")
			_, err := hsw.sendJson(url, msg)
			if err != nil {
				return errors.Wrap(err, "failed to send medadata to htp server")
			}
			// this was only meta, let's continue
		}
	}

	return nil
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
		fd.SetState(lfs.Created)
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
		if fd.GetState() == lfs.Created {
			fd.SetState(lfs.Missing)
		} else {
			fd.SetState(lfs.Diff)
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
		fd.SetState(lfs.Deleted)
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
		if fd.GetState() != lfs.Created {
			fd.SetState(lfs.Meta)
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
		fd.SetState(lfs.Renamed)

	}
	return nil
}
