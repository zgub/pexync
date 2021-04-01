package cmd

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/lfs"
	"github.com/zgub/pexync/workers"
)

func init() {
	rootCmd.AddCommand(clientCmd)
	clientCmd.Flags().StringVarP(&localSource, "local-source", "L", ".", "local sync source")
	clientCmd.Flags().StringVarP(&localDestination, "local-destination", "R", "/tmp/PeXync/", "local sync destination")
}

var (
	localSource, localDestination string

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
	log.Info().Msg("initilaizing PeXync client")
	list, err := lfs.GetList(viper.GetString("directory"))
	if err != nil {
		log.Fatal().
			Err(err).
			Stack().
			Caller().
			Send()
	}
	ctx := context.Background()
	/*
		for _, entry := range list {
			if entry.IsDir {
				log.Info().
					Str("Type", "D").
					//Int64("Size", int64(entry.FileSize)).
					Str("Name", entry.FileName).
					Send()
			} else {
				log.Info().
					Str("Type", "F").
					Int64("Size", int64(entry.FileSize)).
					Str("Name", entry.FileName).
					Send()
			}
		}
	*/

	// call localSync
	startLocalSync(ctx, list)

}

func startLocalSync(ctx context.Context, list []*lfs.FileDesc) {

	// spawn local Sender
	sender := workers.NewLocalSender()
}
