package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
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
}
