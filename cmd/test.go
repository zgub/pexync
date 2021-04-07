package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(testCmd)
}

var (
	//sendMsgType string

	testCmd = &cobra.Command{
		Use:   "test",
		Short: "run test",
		Long:  `runs whatever test actully implemeted`,
		Run: func(cmd *cobra.Command, args []string) {
			runTest()
		},
	}
)

func runTest() {
	log.Info().
		Msgf("PeXync version: %s", Version)
}
