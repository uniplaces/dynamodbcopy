package copytable

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	cmdName          = "copy-table"
	shortDescription = "Copies dynamoDB records from a source to a target table"
)

func New(config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%s <source-table> <target-table>", cmdName),
		Short: shortDescription,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
		},
	}

	cmd.Flags().StringP("source-region", "s", "eu-west-1", "Set the AWS region for the source table")
	cmd.Flags().StringP("target-region", "t", "eu-west-1", "Set the AWS region for the target table")
	cmd.Flags().IntP("read-provision", "r", 0, "Set the read provision to set for the source table")
	cmd.Flags().IntP("write-provision", "w", 0, "Set the write provision to set for the target table")
	if err := config.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}

	return cmd
}
