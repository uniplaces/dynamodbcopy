package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

const cmdName = "dynamodbcopy"

func New(config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: cmdName,
	}

	cmd.AddCommand(
		copytable.New(config),
	)

	return cmd
}
