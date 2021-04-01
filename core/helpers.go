package core

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (e Error) Error() string {
	return string(e)
}

func (e Error) Handle() {
	if e == "" {
		return
	}
	switch e {
	case UnknownMessage:
		e.fatality()
	case NotImplemented:
		e.fatality()
	case Timeout:
		e.fatality()
	default:
		errors.Wrap(e, "unknown error")
	}
}

func (e *Error) fatality() {
	if log.Logger.GetLevel() == zerolog.DebugLevel {
		log.Fatal().
			Stack().
			Err(e).
			Send()
	} else {
		log.Fatal().
			Err(e).
			Send()
	}
}
