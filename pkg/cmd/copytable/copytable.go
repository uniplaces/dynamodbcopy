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

const (
	srcTableKey      = "source-table"
	trgTableKey      = "target-table"
	srcProfileKey    = "source-profile"
	trgProfileKey    = "target-profile"
	readCapacityKey  = "read-capacity"
	writeCapacityKey = "write-capacity"
	readerCountKey   = "reader-count"
	writerCountKey   = "writer-count"
)

func New(config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%s <source-table> <target-table>", cmdName),
		Short: shortDescription,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			config.SetDefault(srcTableKey, args[0])
			config.SetDefault(trgTableKey, args[1])

			deps, err := wireDependencies(config)
			if err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}

			if err := RunCopyTable(deps); err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}
		},
	}

	if err := SetAndBindFlags(cmd.Flags(), config); err != nil {
		panic(err)
	}

	return cmd
}

type Deps struct {
	Copier      dynamodbcopy.Copier
	Provisioner dynamodbcopy.Provisioner
}

func wireDependencies(config *viper.Viper) (Deps, error) {
	srcTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(srcTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(srcProfileKey)),
		dynamodbcopy.RandomSleeper,
	)
	trgTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(trgTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(trgProfileKey)),
		dynamodbcopy.RandomSleeper,
	)

	copier := dynamodbcopy.NewCopier(
		srcTableService,
		trgTableService,
		config.GetInt(readerCountKey),
		config.GetInt(writerCountKey),
	)
	provisioner := dynamodbcopy.NewProvisioner(srcTableService, trgTableService)

	return Deps{Copier: copier, Provisioner: provisioner}, nil
}

func SetAndBindFlags(flagSet *pflag.FlagSet, config *viper.Viper) error {
	flagSet.StringP(srcProfileKey, "s", "", "Set the profile to use for the source table")
	flagSet.StringP(trgProfileKey, "t", "", "Set the profile to use for the target table")
	flagSet.Int(readCapacityKey, 0, "Set the read provisioned capacity for the source table")
	flagSet.Int(writeCapacityKey, 0, "Set the write provisioned capacity for the target table")
	flagSet.IntP(readerCountKey, "r", 1, "Set the number of read workers to use")
	flagSet.IntP(writerCountKey, "w", 1, "Set the number of write workers to use")

	return config.BindPFlags(flagSet)
}

func RunCopyTable(deps Deps) error {
	initialProvisioning, err := deps.Provisioner.Fetch()
	if err != nil {
		return err
	}

	if _, err = deps.Provisioner.Update(initialProvisioning); err != nil {
		return err
	}

	if err := deps.Copier.Copy(); err != nil {
		return err
	}

	if _, err := deps.Provisioner.Update(initialProvisioning); err != nil {
		return err
	}

	return nil
}
