package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var (
	//sendMsgType string

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "print version information",
		Long:  `Prints version information`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersion()
		},
	}
)

func printVersion() {
	log.Info().
		Msgf("PeXync version: %s", Version)
}
