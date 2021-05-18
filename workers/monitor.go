package workers

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// FsEvent is a wrapper struct aroutn fsnotify event
type FsEvent struct {
	fsnotify.Event
}

// Monitor represents a PeXync file monitor
type Monitor struct {
	events  map[int64]FsEvent
	watcher *fsnotify.Watcher
}

// NewMonitor creates a new instance of PeXync filesystem monitor
func NewMonitor() (Monitor, error) {
	mon := Monitor{
		events: make(map[int64]FsEvent),
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return mon, errors.Wrap(err, "unable to initialize fs watcher")
	}
	mon.watcher = w
	return mon, nil
}

func (m Monitor) eval(event fsnotify.Event) {
	if event.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", event.Name).
			Msg("WRT")
	}
	if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", event.Name).
			Msg("REM")
	}
	if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", event.Name).
			Msg("CHD")
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
