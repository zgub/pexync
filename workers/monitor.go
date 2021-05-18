package workers

import (
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
)

// FsEvent is a wrapper struct aroutn fsnotify event
type FsEvent struct {
	fsnotify.Event
}

// Monitor represents a PeXync file monitor
type Monitor struct {
	Events map[int64]FsEvent
}

// NewMonitor creates a new instance of PeXync filesystem monitor
func NewMonitor() Monitor {
	return Monitor{
		Events: make(map[int64]FsEvent),
	}
}

func (m Monitor) Eval(event fsnotify.Event) {
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
