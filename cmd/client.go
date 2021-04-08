package cmd

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
	"github.com/zgub/pexync/workers"
)

func init() {
	rootCmd.AddCommand(clientCmd)
	clientCmd.Flags().StringVarP(&localDestination, "local-destination", "R", "./Xync/", "local sync destination")
	viper.BindPFlag("local_destination", clientCmd.Flags().Lookup("local-destination"))
}

var (
	localDestination string

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
	log.Info().Msg("initializing PeXync client")
	list, err := lfs.GetList(viper.GetString("directory"))
	if err != nil {
		log.Fatal().
			Err(err).
			Stack().
			Caller().
			Send()
	}
	ctx := context.Background()
	startLocalSync(ctx, list)

}

func startLocalSync(ctx context.Context, list []*lfs.FileDesc) {

	var wg sync.WaitGroup
	// spawn local Sender
	local := make(chan *core.Message)
	remote := make(chan *core.Message)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sender := workers.NewLocalSender(ctx, &wg, list, local, remote)

	go sender.Start()
	wg.Add(1)

	receiver := workers.NewLocalReceiver(ctx, &wg, remote, local)
	go receiver.Start()
	wg.Add(1)

	wg.Wait()

}
