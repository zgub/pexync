package cmd

import (
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/test"
)

func init() {

	testCmd.Flags().StringVarP(&scenario, "screnario", "s", "C", "test help scenarion")
	viper.BindPFlag("scenario", testCmd.Flags().Lookup("scenario"))

	rootCmd.AddCommand(testCmd)
}

var (
	scenario string
	testCmd  = &cobra.Command{
		Use:   "test",
		Short: "run some test tasks",
		Long:  `mostly generates some test files for sync tests`,
		Run: func(cmd *cobra.Command, args []string) {
			testTasks()
		},
	}
)

func testTasks() {
	switch scenario {
	case "C":
		createTestFiles()
	case "B":
		test.ReadBenchmark()
	default:
		log.Fatal().
			Msg("unknown scenario")
	}
}

func createTestFiles() {
	if err := os.Remove("testfiles/test-file"); err != nil {
		log.Warn().
			Msgf("no file to delete: %s", err.Error())
	}

	if err := os.Remove("Xync/test-file"); err != nil {
		log.Warn().
			Msgf("no file to delete: %s", err.Error())
	}
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
