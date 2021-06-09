package workers

import (
	"fmt"
	"path/filepath"
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

			fmt.Printf("N E W  *** E V E N T %s for %s\n", event.Op.String(), event.Name)
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
				Msg("Monitor - sync state check")
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
	freeReaders := 0
	toBeSynced := make([]*lfs.FileDesc, 0)
	// first ceck whether there are free readers
	// and colled files that need to be synced in the same run
	for _, fd := range hsw.syncStatus {
		if fd.GetState() == lfs.InSync {
			freeReaders++
		}
		// if it reaches ccIo return, no free readeers
		if freeReaders == ccIo {
			return nil
		}

		if fd.GetState() != lfs.InSync && fd.GetState() != lfs.Synced {
			toBeSynced = append(toBeSynced, fd)
		}
	}

	// here we should have at least one free readers
	// check if there any files to be sent, quit otherwise
	if len(toBeSynced) == 0 {
		// no files to sync
		return nil
	}

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
			// chane the creatd state to missing state, recever was notified
			fd.SetState(lfs.Missing)
		case lfs.Missing:
			// sending whole file
		case lfs.Diff:
			// sending delta, or new file, the bussiness
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
		// lfs.New???
		fd.SetState(lfs.Diff)
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
		fd.SetState(lfs.Meta)
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
