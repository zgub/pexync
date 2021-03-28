package main

import (
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/cmd"
	"github.com/zgub/pexync/core"
)

func main() {

	viper.AutomaticEnv()
	viper.SetEnvPrefix("PXS")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// TODO #1
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().
		Str("version", cmd.Version).
		Msg(cmd.AppName)
	fd, err := core.GetFileDesc("test/testfile")
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error")
	}
	_, err = core.Roll(fd, "test/testfile")
	spew.Dump(fd)
}
