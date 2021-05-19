package workers

import (
	"fmt"
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
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

	for _, fd := range watchList {
		mon.watchMap[filepath.Join(fd.Prefix, fd.FileName)] = fd
	}

	mon.watcher = w
	return mon, nil
}

func (m Monitor) eval(event fsnotify.Event) {
	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("WRT")
		fd, err := lfs.Scan(event.Name)
		if err != nil {
			log.Error().
				Err(err).
				Msg("file stat error")
		}
		spew.Dump(fd)
	}
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("REM")
	}
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("CHM")
	}
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", event.Name).
			Msg("CRT")
	}
	if event.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", event.Name).
			Msg("MOV")
	}
}

func (m Monitor) Start() error {

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
