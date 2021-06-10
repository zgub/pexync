package cmd

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/workers"
	"golang.org/x/sync/errgroup"
)

var (
	//dstHost    string - exists already in client file
	pollInterval int
	monitorCmd   = &cobra.Command{
		Use:   "monitor",
		Short: "synchronize given directory with remote PeXync server and monitor fs changes",
		Long:  `The client command attempts to connect to a PeXync server and synchronize the directory content in an optimized way and monitor fs changes.`,
		Run: func(cmd *cobra.Command, args []string) {
			startMonitor()
		},
	}
)

func init() {
	monitorCmd.Flags().StringVarP(&dstHost, "remote-host", "H", "127.0.0.1", "remote sync destination host")
	err := viper.BindPFlag("remote_host", monitorCmd.Flags().Lookup("remote-host"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}

	viper.SetDefault("poll_interval", 5)

	rootCmd.AddCommand(monitorCmd)
}

func startMonitor() error {

	log.Info().
		Msg("starting remote sync")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	senderID := uuid.New()
	httpSender, err := workers.NewHttpSender(ctx, senderID, false)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to create http sender")
	}
	err = httpSender.Start()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to start http sender")
	}

	log.Info().
		Msg("Starting monitor")

	eg := new(errgroup.Group)
	eg.Go(httpSender.StartMon)

	err = eg.Wait()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("fs watcher error")
	}

	return nil
}
