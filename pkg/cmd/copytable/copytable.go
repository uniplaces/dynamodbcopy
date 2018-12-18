package copytable

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uniplaces/dynamodbcopy"
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
			config.SetDefault("source-table", args[0])
			config.SetDefault("target-table", args[1])

			service, err := dynamodbcopy.NewDynamoDBCopy(*config)
			if err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}

			if err := Run(service); err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}
		},
	}

	if err := SetAndBindFlags(cmd.Flags(), config); err != nil {
		panic(err)
	}

	return cmd
}

func SetAndBindFlags(flagSet *pflag.FlagSet, config *viper.Viper) error {
	flagSet.StringP("source-profile", "s", "", "Set the profile to use for the source table")
	flagSet.StringP("target-profile", "t", "", "Set the profile to use for the target table")
	flagSet.IntP("read-units", "r", 0, "Set the read provisioned capacity for the source table")
	flagSet.IntP("write-units", "w", 0, "Set the write provisioned capacity for the target table")

	return config.BindPFlags(flagSet)
}

func Run(service dynamodbcopy.DynamoDBCopy) error {
	initialProvision, err := service.FetchProvisioning()
	if err != nil {
		return err
	}

	copyProvision := service.CalculateCopyProvisioning(initialProvision)
	if err := service.UpdateProvisioning(copyProvision); err != nil {
		return err
	}

	if err := service.Copy(); err != nil {
		return err
	}

	return service.UpdateProvisioning(initialProvision)
}
