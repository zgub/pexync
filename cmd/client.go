package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/workers"
)

var (
	clientCmd = &cobra.Command{
		Use:   "client",
		Short: "synchronize given directory with remote PeXync server",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way.`,
		Run: func(cmd *cobra.Command, args []string) {
			startClient()
		},
	}
)

func startClient() {

	dstDir := viper.GetString("destination")

	log.Info().
		Str("destination set", dstDir).
		Msg("starting")

	ctx := context.Background()
	log.Info().
		Msg("starting remote sync")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpSender := workers.NewHttpSender(ctx)
	err := httpSender.Start()
	if err != nil {
		log.Error().
			Err(err).
			Msg("error")
	}

}
