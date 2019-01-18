package copytable

import (
	"fmt"

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
	srcRoleArnKey    = "source-role-arn"
	trgRoleArnKey    = "target-role-arn"
	readCapacityKey  = "read-capacity"
	writeCapacityKey = "write-capacity"
	readerCountKey   = "reader-count"
	writerCountKey   = "writer-count"
)

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%s <source-table> <target-table>", cmdName),
		Short: shortDescription,
		Args:  cobra.ExactArgs(2),
		RunE:  runHandler,
	}

	bindFlags(cmd.Flags())

	return cmd
}

func bindFlags(flagSet *pflag.FlagSet) {
	flagSet.StringP(srcRoleArnKey, "s", "", "role arn that allows to read from source table")
	flagSet.StringP(trgRoleArnKey, "t", "", "role arn that allows to write to target table")
	flagSet.Int(readCapacityKey, 0, "read provisioning capacity to set on the source table")
	flagSet.Int(writeCapacityKey, 0, "write provisioning capacity to set on the target table")
	flagSet.IntP(readerCountKey, "r", 1, "number of read workers to use")
	flagSet.IntP(writerCountKey, "w", 1, "number of write workers to use")
}

func runHandler(cmd *cobra.Command, args []string) error {
	deps, err := setupDependencies(cmd, args)
	if err != nil {
		return handleError("error setting up dependencies", err)
	}

	return run(deps)
}

func run(deps dependencies) error {
	initialProvisioning, err := deps.Provisioner.Fetch()
	if err != nil {
		return handleError("error fetching initial provisioning", err)
	}

	updateProvisioning := deps.Config.Provisioning(initialProvisioning)
	if _, err := deps.Provisioner.Update(updateProvisioning); err != nil {
		return handleError("error setting up provisioning before copy", err)
	}

	if err := deps.Copier.Copy(deps.Config.Workers()); err != nil {
		copyErr := handleError("error copying records", err)
		if _, provisionErr := deps.Provisioner.Update(initialProvisioning); provisionErr != nil {
			return handleError(copyErr.Error(), provisionErr)
		}

		return copyErr
	}

	if _, err := deps.Provisioner.Update(initialProvisioning); err != nil {
		return handleError("error restoring initial provisioning", err)
	}

	return nil
}

func handleError(msg string, err error) error {
	return fmt.Errorf("[%s] %s\n%s", cmdName, msg, err)
}

type dependencies struct {
	Copier      dynamodbcopy.Copier
	Provisioner dynamodbcopy.Provisioner
	Config      dynamodbcopy.Config
}

func setupDependencies(cmd *cobra.Command, args []string) (dependencies, error) {
	config := viper.New()

	config.SetDefault(srcTableKey, args[0])
	config.SetDefault(trgTableKey, args[1])

	if err := config.BindPFlags(cmd.Flags()); err != nil {
		return dependencies{}, err
	}

	srcTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(srcTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(srcRoleArnKey)),
		dynamodbcopy.RandomSleeper,
	)
	trgTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(trgTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(trgRoleArnKey)),
		dynamodbcopy.RandomSleeper,
	)

	copier := dynamodbcopy.NewCopier(srcTableService, trgTableService)
	provisioner := dynamodbcopy.NewProvisioner(srcTableService, trgTableService)

	return dependencies{
		Copier:      copier,
		Provisioner: provisioner,
		Config: dynamodbcopy.NewConfig(
			config.GetInt(readCapacityKey),
			config.GetInt(writeCapacityKey),
			config.GetInt(readerCountKey),
			config.GetInt(writerCountKey),
		),
	}, nil
}
