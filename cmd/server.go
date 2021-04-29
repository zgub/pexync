package cmd

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/workers"
)

func init() {
	fmt.Println("dot")

	serverCmd.Flags().StringVarP(&bindAddr, "bind-address", "B", "0.0.0.0", "IP address")
	viper.BindPFlag("bind_address", serverCmd.Flags().Lookup("bind-address"))

	rootCmd.AddCommand(serverCmd)
	fmt.Println("dot1")
}

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

func startServer() {
	log.Info().
		Msg("initializing PeXync server")
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		remote, local chan *core.Message
	)
	fmt.Println("dot2")
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
