package cmd

import (
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/zgub/pexync/test"
)

const (
	X_X int = iota
	X_0
	XXX_YYY
	XXX_000
)

func init() {

	testCmd.Flags().StringVarP(&scenario, "scenario", "s", "C", "test help scenarion")

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
	case "C1":
		createTestFiles(X_X)
	case "C2":
		createTestFiles(XXX_000)
	case "C3":
		createTestFiles(XXX_YYY)
	case "B":
		test.ReadBenchmark()
	case "R":
		srcS := "123x456y789z0?!"
		dstS := "123789u"
		testS := "@#$%^123tt789u"

		test.RollV3(srcS, dstS)

		for _, s := range testS {
			srcS += string(s)
			test.RollV3(srcS, dstS)
		}
	case "RP":
		hitList, err := test.CreateRandPair(700, 10)
		if err != nil {
			log.Fatal().
				Err(err).
				Caller().
				Msg("failed to create files")
		}
		spew.Dump(hitList)
	default:
		log.Fatal().
			Msg("unknown scenario")
	}
}

func createTestFiles(mode int) {
	switch mode {
	case X_X:
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
	case XXX_000:
		fn, err := test.CreateTestFile("testfiles/", "", 700, 3, test.AABBCC)
		if err != nil {
			log.Fatal().
				Msgf("failed to create a test file %s", err.Error())
		}
		log.Info().
			Str("file name", fn).
			Msg("created")
		fn, err = test.CreateTestFile("testFiles/", "", 700, 3, test.BBCCDD)
		if err != nil {
			log.Fatal().
				Msgf("failed to create a test file %s", err.Error())
		}
		log.Info().
			Str("file name", fn).
			Msg("created")
		fn, err = test.CreateTestFile("testFiles/", "", 700, 3, test.AACCEE)
		if err != nil {
			log.Fatal().
				Msgf("failed to create a test file %s", err.Error())
		}
		log.Info().
			Str("file name", fn).
			Msg("created")
	case XXX_YYY:
		fn, err := test.CreateTestFile("testfiles/", "", 700, 5, test.AABBCC)
		if err != nil {
			log.Fatal().
				Msgf("failed to create a test file %s", err.Error())
		}
		err = os.Rename("testfiles/5x700-test-file-AABBCC", "testfiles/test-file")
		if err != nil {
			log.Fatal().
				Msgf("failed to rename test file %s", err.Error())
		}
		log.Info().
			Str("file name", fn).
			Msg("created")
		fn, err = test.CreateTestFile("Xync/", "", 700, 3, test.AACCEE)
		if err != nil {
			log.Fatal().
				Msgf("failed to create a test file %s", err.Error())
		}
		err = os.Rename("Xync/3x700-test-file-AACCEE", "Xync/test-file")
		if err != nil {
			log.Fatal().
				Msgf("failed to rename test file %s", err.Error())
		}
		log.Info().
			Str("file name", fn).
			Msg("created")
	}
}
