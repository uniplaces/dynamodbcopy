package cmd

import (
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

const cmdName = "dynamodbcopy"

// New creates the root dynamodbcopy command
func New() *cobra.Command {
	cmd := &cobra.Command{
		Use: cmdName,
	}

	cmd.AddCommand(
		copytable.New(log.New(os.Stdout, "", log.LstdFlags)),
	)

	return cmd
}
