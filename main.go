package main

import (
	"io/ioutil"
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

	viper.AutomaticEnv()
	viper.SetEnvPrefix("PXS")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// TODO #1
	logFile, err := ioutil.TempFile(".", "PeXync.*.log")
	if err != nil {
		// Can we log an error before we have our logger? :)
		log.Error().Err(err).Msg("there was an error creating a temporary file four our log")
	}
	defer logFile.Close()
	//log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	multi := zerolog.MultiLevelWriter(consoleWriter, logFile)
	log.Logger = log.Output(multi)
	//logger := zerolog.New(multi).With().Timestamp().Logger()

	log.Info().
		Str("version", cmd.Version).
		Msg(cmd.AppName)

	start := time.Now()

	cmd.Execute()
	log.Info().
		TimeDiff("duration", time.Now(), start).
		Send()

}
