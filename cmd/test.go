package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var (
	testCmd = &cobra.Command{
		Use:   "test",
		Short: "run some test tasks",
		Long:  `mostly generates some test files for sync tests`,
		Run: func(cmd *cobra.Command, args []string) {
			testTasks()
		},
	}
)

func testTasks() {
	log.Info().
		Msgf("PeXync version: %s", Version)
}
