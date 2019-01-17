package cmd

import (
	"github.com/spf13/cobra"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

const cmdName = "dynamodbcopy"

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use: cmdName,
	}

	cmd.AddCommand(
		copytable.New(),
	)

	return cmd
}
