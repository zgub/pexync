package core

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (e Error) Error() string {
	return string(e)
}

func (e Error) Handle() {
	fmt.Printf("meh: %s\n", e.Error())
	if e == "" {
		return
	}
	switch e {
	case UnknownMessage:
		e := errors.WithStack(e)
		Fatality(e)
	case NotImplemented:
		e := errors.WithStack(e)
		Fatality(e)
	case Timeout:
		e := errors.WithStack(e)
		Fatality(e)
	default:
		errors.WithStack(errors.Wrap(e, "unknown error"))
	}
}

func Fatality(e error) {
	if e == nil {
		return
	}
	fmt.Println("FATALITY")
	if zerolog.GlobalLevel() == zerolog.DebugLevel || zerolog.GlobalLevel() == zerolog.TraceLevel {
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
