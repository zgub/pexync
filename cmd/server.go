package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/workers"
)

func init() {
	rootCmd.AddCommand(serverCmd)
}

var (
	serverCmd = &cobra.Command{
		Use:   "client",
		Short: "synchronize given directory with remote PeXync server",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way.`,
		Run: func(cmd *cobra.Command, args []string) {
			startServer()
		},
	}
)

func startServer() {
	log.Info().Msg("initializing PeXync server")
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		remote, local chan *core.Message
	)
	receiver := workers.NewHttpReceiver(ctx, remote, local)
	err := receiver.Start()

	if err == nil {
		log.Info().
			Msg("sync finished")
	} else {
		log.Error().
			Err(err).
			Send()
	}

}
