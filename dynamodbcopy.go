package dynamodbcopy

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func NewDynamoDBCopy(copyConfig Config, srcTableService, trgTableService DynamoDBService) (DynamoDBCopy, error) {
	return dynamodbCopy{
		config:   copyConfig,
		srcTable: srcTableService,
		trgTable: trgTableService,
	}, nil
}

func (dc dynamodbCopy) FetchProvisioning() (TablesDescription, error) {
	srcDescription, err := dc.srcTable.DescribeTable()
	if err != nil {
		return TablesDescription{}, err
	}

	trgDescription, err := dc.trgTable.DescribeTable()
	if err != nil {
		return TablesDescription{}, err
	}

	return TablesDescription{Source: *srcDescription, Target: *trgDescription}, nil
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
