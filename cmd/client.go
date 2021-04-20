package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
	"github.com/zgub/pexync/workers"
	"golang.org/x/sync/errgroup"
)

func init() {
	rootCmd.AddCommand(clientCmd)
	clientCmd.Flags().StringVarP(&localDestination, "local-destination", "L", "", "local destination")
	viper.BindPFlag("local_destination", clientCmd.Flags().Lookup("local-destination"))

	clientCmd.Flags().StringVarP(&remoteDestination, "remote-destination", "R", "", "remote destination")
	viper.BindPFlag("remote_destination", clientCmd.Flags().Lookup("remote-destination"))

}

var (
	localDestination, remoteDestination string

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

	localDst := viper.GetString("local_destination")
	if localDst != "" {

		log.Info().
			Str("local destination set", localDst).
			Msg("starting local sync")

		list, err := lfs.ParseDir(viper.GetString("directory"))
		if err != nil {
			log.Fatal().
				Err(err).
				Stack().
				Caller().
				Send()
		}
		ctx := context.Background()
		startLocalSync(ctx, list)
	} else {
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

}

func startLocalSync(ctx context.Context, list []*lfs.FileDesc) {

	g := new(errgroup.Group)
	local := make(chan *core.Message)
	remote := make(chan *core.Message)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sender := workers.NewLocalSender(ctx, list, local, remote)

	g.Go(func() error { return sender.Start() })

	receiver := workers.NewLocalReceiver(ctx, remote, local)
	g.Go(func() error { return receiver.Start() })

	if err := g.Wait(); err == nil {
		log.Info().
			Msg("local sync done")
	} else {
		log.Error().
			Err(err).
			Send()
	}

}
