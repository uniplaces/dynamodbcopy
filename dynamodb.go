package dynamodbcopy

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDBService interface {
	DescribeTable() (*dynamodb.DescribeTableOutput, error)
	UpdateCapacity(read, write int64) error
}

type dynamoDB struct {
	tableName string
	profile   string
	api       *dynamodb.DynamoDB
}

func NewDynamoDB(tableName string, profile string) DynamoDBService {
	options := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	if profile != "" {
		options.Profile = profile
	}

	api := dynamodb.New(
		session.Must(
			session.NewSessionWithOptions(
				options,
			),
		),
	)

	return dynamoDB{tableName, profile, api}
}

func (db dynamoDB) DescribeTable() (*dynamodb.DescribeTableOutput, error) {
	return nil, nil
}

func (db dynamoDB) UpdateCapacity(read, write int64) error {
	return nil
}
