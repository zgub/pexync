package cmd

import (
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
	Version = "v0.0.1"
	// AppShortDesc is the application short description
	AppShortDesc = "] pexip [ homework"
	// AppDesc is the application long description
	AppDesc = `Lazy golang rsync implementation`
)

var (
	// general
	useCores int
	cfgFile  string
	debug    bool
	port     int
	syncDir  string

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
	cobra.OnInitialize(initConfig)

	// common flags
	rootCmd.PersistentFlags().IntVarP(&blockSize, "block-size", "b", 700, "block size in bytes, default <700b; 131kB>")
	viper.BindPFlag("block_size", rootCmd.PersistentFlags().Lookup("block-size"))

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")

	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "D", false, "enable debug")

	viper.SetDefault("log_level", int(zerolog.InfoLevel))

	rootCmd.PersistentFlags().StringVarP(&syncDir, "directory", "d", ".", "directory to synchronize")
	viper.BindPFlag("directory", rootCmd.PersistentFlags().Lookup("directory"))

	rootCmd.PersistentFlags().IntVarP(&port, "port", "p", 8080, "http API port")

	viper.SetDefault("timeout", 5*time.Second)
}

// Execute executes the root command.
func Execute() error {
	/*
		if useCores != 0 {
			numCores := runtime.GOMAXPROCS(useCores)
			log.Info().
				Msgf("Cores used: %v -> %v", numCores, useCores)
		}
	*/
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
			Msg("CONFIG")
	} else {
		viper.SetConfigType("toml")
		err := viper.SafeWriteConfig()
		if err != nil {
			log.Error().
				Str("Error", err.Error()).
				Msg("unable to write config")
		}
	}

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("debug mode on")
	} else {
		level := zerolog.Level(viper.GetInt("log_level"))
		zerolog.SetGlobalLevel(level)
		log.Info().
			Str("log level", level.String()).
			Send()
	}
}
