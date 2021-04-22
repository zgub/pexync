package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/zgub/pexync/test"
)

func init() {
	rootCmd.AddCommand(testCmd)
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
	fn, err := test.CreateTestFile("testfiles/", "test-file", 700, 7, test.AABBCC)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error creating test files")
	}
	log.Info().
		Str("file name", fn).
		Msg("created")

	fn, err = test.CreateTestFile("Xync/", "test-file", 700, 4, test.AACCEE)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error creating test files")
	}
	log.Info().
		Str("file name", fn).
		Msg("created")
}
