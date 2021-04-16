package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/zgub/pexync/core"
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
	err := core.CreateTestFile(700, 4)
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to create test file")
	}
	err = core.CreateTestFile(700, 6)
	if err != nil {
		log.Error().
			Err(err).
			Msg("unable to create test file")
	}
}
