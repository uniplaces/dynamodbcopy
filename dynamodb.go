package dynamodbcopy

import (
	"fmt"

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
	UpdateCapacity(capacity Capacity) error
	WaitForReadyTable() error
}

type dynamoDBSerivce struct {
	tableName string
	api       DynamoDBAPI
	sleep     Sleeper
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

func NewDynamoDBService(tableName string, api DynamoDBAPI, sleepFn Sleeper) DynamoDBService {
	return dynamoDBSerivce{tableName, api, sleepFn}
}

func (db dynamoDBSerivce) DescribeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}

	output, err := db.api.DescribeTable(input)
	if err != nil {
		return nil, err
	}

	return output.Table, nil
}

func (db dynamoDBSerivce) UpdateCapacity(capacity Capacity) error {
	read := capacity.Read
	write := capacity.Write

	if read == 0 || write == 0 {
		return fmt.Errorf(
			"invalid update capacity read %d & write %d: capacity units must be greater than 0",
			read,
			write,
		)
	}

	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(db.tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(read),
			WriteCapacityUnits: aws.Int64(write),
		},
	}

	_, err := db.api.UpdateTable(input)
	if err != nil {
		return err
	}

	return db.WaitForReadyTable()
}

func (db dynamoDBSerivce) WaitForReadyTable() error {
	elapsed := 0

	for attempt := 0; ; attempt++ {
		description, err := db.DescribeTable()
		if err != nil {
			return err
		}

		if *description.TableStatus == dynamodb.TableStatusActive {
			break
		}

		elapsed += db.sleep(elapsed * attempt)
	}

	return nil
}
