package cmd

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
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

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to initialize")
	}

	list, err := lfs.ParseDir(viper.GetString("source"))
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("monitor - direcotry parse failed")
	}

	for _, fd := range list {
		if fd.IsDir {
			log.Trace().
				Str("path", fd.FileName).
				Msg("will be monitored")
		}
	}

	eg := new(errgroup.Group)
	eg.Go(func() error {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return errors.New("an error occurred while watching directory")
				}

				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Trace().
						Str("modified file:", event.Name).
						Msg("*** write event")
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return errors.New("an error occurred while watching directory")
				}
				return err
			}
		}
	})

	srcDir := viper.GetString("source")
	path, err := filepath.Abs(srcDir)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to watch directory")
	}
	err = watcher.Add(path)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("unable to add direcotry to watchlist")
	}
	err = eg.Wait()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("fsnotify watcher error")
	}
}
