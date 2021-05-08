package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(cfgGenCmd)
}

var (
	//sendMsgType string

	cfgGenCmd = &cobra.Command{
		Use:   "confgen",
		Short: "generate configuretion file",
		Long:  `generates a configuration file`,
		Run: func(cmd *cobra.Command, args []string) {
			cfgGen()
		},
	}
)

func cfgGen() {
	viper.SetConfigType("toml")
	err := viper.SafeWriteConfig()
	if err != nil {
		log.Error().
			Str("Error", err.Error()).
			Msg("unable to write config")
	}
	log.Info().
		Msgf("Config file generated %s.toml", AppName)
}
