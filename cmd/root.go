package cmd

import (
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
	useCores   int
	cfgFile    string
	debugLevel int
	port       int
	syncDir    string

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

	rootCmd.PersistentFlags().IntVarP(&debugLevel, "log-level", "D", 0, "log level: 0 - Error, 1 - Warn, 2 - Info, 3 - debug, 4 - trace")
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))

	rootCmd.PersistentFlags().StringVarP(&syncDir, "directory", "d", ".", "directory to synchronize")
	viper.BindPFlag("directory", rootCmd.PersistentFlags().Lookup("directory"))

	rootCmd.PersistentFlags().IntVarP(&port, "port", "p", 8080, "http API port")

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
}
