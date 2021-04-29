package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/workers"
)

var (
	bindAddr  string
	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "synchronize given directory with remote PeXync server",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way.`,
		Run: func(cmd *cobra.Command, args []string) {
			startServer()
		},
	}
)

func init() {

	serverCmd.Flags().StringVarP(&bindAddr, "bind-address", "B", "127.0.0.1", "ip address")
	viper.BindPFlag("bind_address", serverCmd.Flags().Lookup("bind-address"))
	//viper.SetDefault("bind_address", "127.0.0.1")

	rootCmd.AddCommand(serverCmd)

}

func startServer() {
	log.Info().
		Msg("Starting PeXync server")
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	receiver := workers.NewHttpReceiver(ctx)
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
