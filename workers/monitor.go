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

// Monitor represents a PeXync file monitor
type Monitor struct {
	watcher    *fsnotify.Watcher
	watchMap   map[string]*lfs.FileDesc
	rrCh, brCh chan *core.Message
	idx        int64
}

// NewMonitor creates a new instance of PeXync filesystem monitor
func NewMonitor(rrCh, brCh chan *core.Message, watchList []*lfs.FileDesc) (Monitor, error) {
	mon := Monitor{
		rrCh:     rrCh,
		brCh:     brCh,
		watchMap: make(map[string]*lfs.FileDesc),
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return mon, errors.Wrap(err, "unable to initialize fs watcher")
	}

	// determine block size and add weak block Adler32 hashlist and Sha1 digest
	for _, fd := range watchList {
		if !fd.IsDir {
			fd.SetBlockSize()
			// beware of empty files
			if fd.BlockSize == 0 {
				fd.BlockSize = 700
			}
			fmt.Println("watchlist")
			err = core.AddChecksums(fd)
			if err != nil {
				return mon, errors.Wrapf(err, "Monitor init - failed to calculate checksums - file: %s", filepath.Join(fd.Prefix, fd.FileName))
			}
		}
		// for new file indexes
		if fd.Idx > mon.idx {
			mon.idx = fd.Idx
		}
		mon.watchMap[filepath.Join(fd.Prefix, fd.FileName)] = fd
	}

	mon.watcher = w
	return mon, nil
}

func (m Monitor) eval(event fsnotify.Event) {

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
		if fd, ok := m.watchMap[event.Name]; ok {
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
					m.watchMap[event.Name] = efd
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
			fmt.Println("event")
			err = core.AddChecksums(efd)
			if err != nil {
				log.Error().
					Err(err).
					Msg("Monitor event - failed to calculate checksums")
				return
			}
		}

		m.idx++
		efd.Idx = m.idx
		m.watchMap[event.Name] = efd
	}
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("RENAME - event detected")
	}

}

func (m Monitor) Start() error {
	log.Info().
		Int64("last file index", m.idx).
		Msg("MONITOR - Starting")

	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}

			m.eval(event)

		case err, ok := <-m.watcher.Errors:
			if !ok {
				return errors.New("an error occurred while watching directory")
			}
			return err
		}
	}
}

func (m Monitor) Watch(path string) error {
	fmt.Printf("adding to watch: %s\n", path)
	return m.watcher.Add(path)
}
