package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/workers"
)

var (
	dstHost   string
	clientCmd = &cobra.Command{
		Use:   "client",
		Short: "synchronize given directory with remote PeXync server",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way.`,
		Run: func(cmd *cobra.Command, args []string) {
			startClient()
		},
	}
)

func init() {
	clientCmd.PersistentFlags().StringVarP(&dstHost, "remote-host", "H", "127.0.0.1", "remote sync destination host")
	viper.BindPFlag("remote_host", clientCmd.PersistentFlags().Lookup("remote-host"))
	rootCmd.AddCommand(clientCmd)
}

func startClient() {

	ctx := context.Background()
	log.Info().
		Msg("starting remote sync")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpSender, err := workers.NewHttpSender(ctx)
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
