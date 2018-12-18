package dynamodbcopy

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DynamoDBAPI just a wrapper over aws-sdk dynamodbiface.DynamoDBAPI interface for mocking purposes
type DynamoDBAPI interface {
	dynamodbiface.DynamoDBAPI
}

type DynamoDBService interface {
	DescribeTable() (*dynamodb.TableDescription, error)
	UpdateCapacity(read, write int64) error
}

type dynamoDB struct {
	tableName string
	api       DynamoDBAPI
}

func NewDynamoDBAPI(profile string) DynamoDBAPI {
	options := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	if profile != "" {
		options.Profile = profile
	}

	return dynamodb.New(
		session.Must(
			session.NewSessionWithOptions(
				options,
			),
		),
	)
}

func NewDynamoDBService(tableName string, api DynamoDBAPI) DynamoDBService {
	return dynamoDB{tableName, api}
}

func (db dynamoDB) DescribeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}

	output, err := db.api.DescribeTable(input)
	if err != nil {
		return nil, err
	}

	return output.Table, nil
}

func (db dynamoDB) UpdateCapacity(read, write int64) error {
	return nil
}
