package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/lfs"
)

func init() {
	rootCmd.AddCommand(clientCmd)
	//sendCmd.Flags().StringVarP(&sendMsgType, "message-type", "t", "", "API meessage type (C|R|U|D|G|I|V)")
}

var (
	//sendMsgType string

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
}
