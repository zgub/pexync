package main

import (
	"os"
	"strings"
	"time"

	//"github.com/davecgh/go-spew/spew"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/cmd"
)

func main() {

	//runtime.GOMAXPROCS(12)
	viper.AutomaticEnv()
	viper.SetEnvPrefix("PXS")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// TODO #1
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().
		Str("version", cmd.Version).
		Msg(cmd.AppName)

	start := time.Now()

	cmd.Execute()
	log.Info().
		TimeDiff("duration", time.Now(), start).
		Send()

}
