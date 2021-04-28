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

	localCmd.PersistentFlags().StringVarP(&dstDir, "destination", "R", "/", "destination directory")
	viper.BindPFlag("destination", localCmd.PersistentFlags().Lookup("destination"))

	rootCmd.AddCommand(localCmd)
}

var (
	dstDir   string
	localCmd = &cobra.Command{
		Use:   "local",
		Short: "synchronize given directory with another local directory",
		Long:  `The local command will synchronize two direcotries on the local host.`,
		Run: func(cmd *cobra.Command, args []string) {
			startLocalSync()
		},
	}
)

func startLocalSync() {

	dstDir := viper.GetString("destination")

	if dstDir == "/" {
		log.Fatal().
			Msg("destination directory not set")
	}

	log.Info().
		Str("local destination set", dstDir).
		Msg("starting local sync")

	list, err := lfs.ParseDir(viper.GetString("source"))
	if err != nil {
		log.Fatal().
			Err(err).
			Stack().
			Caller().
			Send()
	}
	ctx := context.Background()

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
