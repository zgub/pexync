package cmd

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/workers"
)

var (
	//dstHost    string - exists already in client file
	monitorCmd = &cobra.Command{
		Use:   "monitor",
		Short: "synchronize given directory with remote PeXync server and monitor fs changes",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way and monitor fs change s.`,
		Run: func(cmd *cobra.Command, args []string) {
			startMonitor()
		},
	}
)

func init() {
	clientCmd.Flags().StringVarP(&dstHost, "remote-host", "H", "127.0.0.1", "remote sync destination host")
	err := viper.BindPFlag("remote_host", clientCmd.Flags().Lookup("remote-host"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}

	rootCmd.AddCommand(monitorCmd)
}

func startMonitor() {

	uuid := uuid.New()

	ctx := context.Background()
	log.Info().
		Msg("starting remote sync")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	httpSender, err := workers.NewHttpSender(ctx, uuid)
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to create http sender")
	}
	err = httpSender.Start()
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to start http sender")
	}

}
