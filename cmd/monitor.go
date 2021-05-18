package cmd

import (
	"context"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/lfs"
	"github.com/zgub/pexync/workers"
	"golang.org/x/sync/errgroup"
)

var (
	//dstHost    string - exists already in client file
	monitorCmd = &cobra.Command{
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

	// initial sync done

	mon, err := workers.NewMonitor()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to initialize fs watcher")
	}

	eg := new(errgroup.Group)
	eg.Go(mon.Start)

	// reading the source dir again, because we have no easy means of getting it from he sender (lazy)
	srcDir := viper.GetString("source")
	path, err := filepath.Abs(srcDir)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to watch directory")
	}

	list, err := lfs.ParseDir(srcDir)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("monitor - directory parse failed")
	}

	err = mon.Watch(path)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to add direcotry to watchlist")
	}
	log.Info().
		Str("path", path).
		Msg("Monitoring")

	for _, fd := range list {
		if fd.IsDir {
			path = filepath.Join(fd.Prefix, fd.FileName)
			err = mon.Watch(path)
			if err != nil {
				log.Fatal().
					Err(err).
					Str("path", path).
					Msg("failed to initialize directory watcher")
			}
			log.Trace().
				Str("path", fd.FileName).
				Msg("Monitoring")
		}
	}

	err = eg.Wait()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("fs watcher error")
	}
}
