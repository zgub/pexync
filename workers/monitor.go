package workers

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
)

// Monitor represents a PeXync file monitor
type Monitor struct {
	watcher    *fsnotify.Watcher
	rrCh, brCh chan *core.Message
}

// NewMonitor creates a new instance of PeXync filesystem monitor
func NewMonitor(rrCh, brCh chan *core.Message) (Monitor, error) {
	mon := Monitor{
		rrCh: rrCh,
		brCh: brCh,
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return mon, errors.Wrap(err, "unable to initialize fs watcher")
	}
	mon.watcher = w
	return mon, nil
}

func (m Monitor) eval(mEvent fsnotify.Event) {
	if mEvent.Op&fsnotify.Write == fsnotify.Write {
		log.Info().
			Str("path", mEvent.Name).
			Msg("WRT")
	}
	if mEvent.Op&fsnotify.Remove == fsnotify.Remove {
		log.Info().
			Str("path", mEvent.Name).
			Msg("REM")
	}
	if mEvent.Op&fsnotify.Chmod == fsnotify.Chmod {
		log.Info().
			Str("path", mEvent.Name).
			Msg("CHM")
	}
	if mEvent.Op&fsnotify.Create == fsnotify.Create {
		log.Info().
			Str("path", mEvent.Name).
			Msg("CRT")
	}
	if mEvent.Op&fsnotify.Rename == fsnotify.Rename {
		log.Info().
			Str("path", mEvent.Name).
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
