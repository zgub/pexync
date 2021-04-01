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
		e := errors.WithStack(e)
		fatality(e)
	case NotImplemented:
		e := errors.WithStack(e)
		fatality(e)
	case Timeout:
		fatality(e)
	default:
		errors.WithStack(errors.Wrap(e, "unknown error"))
	}
}

func fatality(e error) {
	if zerolog.GlobalLevel() == zerolog.DebugLevel {
		log.Fatal().
			Stack().
			Err(e).
			Caller().
			Send()
	} else {
		log.Fatal().
			Err(e).
			Send()
	}
}
