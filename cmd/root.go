package cmd

import (
	"runtime"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// AppName can be replaced with an application name during build
	AppName = "PeXync"
	// Version is defined during the compilation as well
	Version = "v0.1.0"
	// AppShortDesc is the application short description
	AppShortDesc = "] pexip [ homework"
	// AppDesc is the application long description
	AppDesc = `Lazy golang rsync implementation`
)

var (
	// general
	useCores       int
	cfgFile        string
	debug          bool
	ccIo           int
	srcDir, dstDir string
	port           int

	// core
	blockSize int

	// root command
	rootCmd = &cobra.Command{
		Use:   AppName,
		Short: AppShortDesc,
		Long:  AppDesc,
	}
)

func init() {

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")

	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "D", false, "enable debug")

	viper.SetDefault("log_level", int(zerolog.InfoLevel))

	// common flags
	rootCmd.PersistentFlags().IntVarP(&blockSize, "block-size", "b", 700, "block size in bytes, default <700b; 131kB>")
	err := viper.BindPFlag("block_size", rootCmd.PersistentFlags().Lookup("block-size"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
	viper.SetDefault("use_cores", 1)

	viper.SetDefault("timeout", 5*time.Second)

	rootCmd.PersistentFlags().IntVarP(&ccIo, "io-concurrency", "i", 2, "concurent io operations")
	err = viper.BindPFlag("io_concurrency", rootCmd.PersistentFlags().Lookup("io-concurrency"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
	//viper.SetDefault("io_concurrency", 2)

	rootCmd.PersistentFlags().StringVarP(&srcDir, "source", "S", "testfiles/", "source directory to synchronize")
	err = viper.BindPFlag("source", rootCmd.PersistentFlags().Lookup("source"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
	//viper.SetDefault("source", "testfiles/")

	rootCmd.PersistentFlags().StringVarP(&dstDir, "destination", "R", "/", "destination directory")
	err = viper.BindPFlag("destination", rootCmd.PersistentFlags().Lookup("destination"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
	//viper.SetDefault("destination", "/")

	rootCmd.PersistentFlags().IntVarP(&port, "port", "p", 3819, "http API port")
	err = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	if err != nil {
		log.Fatal().
			Err(err).
			Send()
	}
	//viper.SetDefault("port", 3819)

	cobra.OnInitialize(initConfig)
}

// Execute executes the root command.
func Execute() error {

	return rootCmd.Execute()
}

func initConfig() {
	if cfgFile != "" {
		log.Info().
			Str("config file", cfgFile).
			Msg("Config from command line")
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName(AppName)
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		log.Info().
			Str("Using config file:", viper.ConfigFileUsed()).
			Msg("CFG")
	}

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().
			Msg("debug mode on")
	} else {
		level := zerolog.Level(viper.GetInt("log_level"))
		zerolog.SetGlobalLevel(level)
		log.Info().
			Str("log level", level.String()).
			Msg("LOG")
	}

	if useCores != 1 {
		useCores = viper.GetInt("use_cores")
		runtime.GOMAXPROCS(useCores)
		log.Info().
			Msgf("CPU cores set: %v", useCores)
	}
}
