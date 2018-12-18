package dynamodbcopy

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/spf13/viper"
)

type TablesDescription struct {
	Source dynamodb.TableDescription
	Target dynamodb.TableDescription
}

type DynamoDBCopy interface {
	FetchProvisioning() (TablesDescription, error)
	UpdateProvisioning(descriptions TablesDescription) error
	CalculateCopyProvisioning(descriptions TablesDescription) TablesDescription
	Copy() error
}

type dynamodbCopy struct {
	config   Config
	srcTable DynamoDBService
	trgTable DynamoDBService
}

func NewDynamoDBCopy(config viper.Viper) (DynamoDBCopy, error) {
	copyConfig, err := NewConfig(config)
	if err != nil {
		return nil, err
	}

	return dynamodbCopy{
		config:   copyConfig,
		srcTable: NewDynamoDB(copyConfig.SourceTable, copyConfig.SourceProfile),
		trgTable: NewDynamoDB(copyConfig.TargetTable, copyConfig.TargetProfile),
	}, nil
}

func (dc dynamodbCopy) FetchProvisioning() (TablesDescription, error) {
	return TablesDescription{}, nil
}

func (dc dynamodbCopy) CalculateCopyProvisioning(descriptions TablesDescription) TablesDescription {
	return descriptions
}

func (dc dynamodbCopy) UpdateProvisioning(descriptions TablesDescription) error {
	return nil
}

func (dc dynamodbCopy) Copy() error {
	return nil
}
