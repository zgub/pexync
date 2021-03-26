package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
)

func main() {

	viper.AutomaticEnv()
	viper.SetEnvPrefix("CGL")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// TODO #1
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	fmt.Println("Hello world")
	f, err := os.Open("main.go")
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error")
	}
	sums, err := core.GetChecksums(f, 700)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error")
	}
	spew.Dump(sums)
}
