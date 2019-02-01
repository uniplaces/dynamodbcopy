package dynamodbcopy

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	maxBatchWriteSize = 25
	maxRetryTime      = int(time.Minute) * 3

	errCodeThrottlingException = "ThrottlingException"
)

// DynamoDBClient is a wrapper interface over aws-sdk dynamodbiface.DynamoDBClient for mocking purposes
type DynamoDBClient interface {
	dynamodbiface.DynamoDBAPI
}

// NewDynamoClient creates a DynamoDB client wrapper around the AWS-SDK with a predefined Session.
// By default, it creates a new Session with SharedConfigEnable,
// so you can use AWS SDK's environment variables and AWS credentials to connect to DynamoDB.
//
// The provided ARN role allows you to configure the Session to assume a specific IAM Role.
//
// If an empty string is provided, it will create a new session with SharedConfigEnable
// Please refer to https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html for more
// information on how you can set up the SDK
func NewDynamoClient(roleArn string) DynamoDBClient {
	options := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	currentSession := session.Must(session.NewSessionWithOptions(options))
	if roleArn != "" {
		roleCredentials := stscreds.NewCredentials(currentSession, roleArn)

		return dynamodb.New(currentSession, &aws.Config{Credentials: roleCredentials})
	}

	return dynamodb.New(currentSession)
}

// DynamoDBItem type to abstract a DynamoDB item
type DynamoDBItem map[string]*dynamodb.AttributeValue

// DynamoDBService interface provides methods to call the aws sdk
type DynamoDBService interface {
	DescribeTable() (*dynamodb.TableDescription, error)
	UpdateCapacity(capacity Capacity) error
	WaitForReadyTable() error
	BatchWrite(items []DynamoDBItem) error
	Scan(totalSegments, segment int, itemsChan chan<- []DynamoDBItem) error
}

type dynamoDBSerivce struct {
	tableName string
	client    DynamoDBClient
	sleep     Sleeper
	logger    Logger
}

// NewDynamoDBService creates new service for a given DynamoDB table with a previously configured DynamoDB client
func NewDynamoDBService(tableName string, client DynamoDBClient, sleepFn Sleeper, logger Logger) DynamoDBService {
	return dynamoDBSerivce{tableName, client, sleepFn, logger}
}

// DescribeTable returns the current table metadata for the DynamoDB table
func (db dynamoDBSerivce) DescribeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}

	output, err := db.client.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("unable to describe table %s: %s", db.tableName, err)
	}

	return output.Table, nil
}

// UpdateCapacity sets the tables read and write capacity, waiting for the table to be ready for processing
func (db dynamoDBSerivce) UpdateCapacity(capacity Capacity) error {
	read := capacity.Read
	write := capacity.Write

	if read == 0 || write == 0 {
		return fmt.Errorf(
			"invalid update capacity read %d, write %d: capacity units must be greater than 0",
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

	db.logger.Printf("updating %s with read: %d, write: %d", db.tableName, read, write)
	_, err := db.client.UpdateTable(input)
	if err != nil {
		return fmt.Errorf("unable to update table %s: %s", db.tableName, err)
	}

	return db.WaitForReadyTable()
}

// BatchWrite writes the given DynamoDBItem slice into the DynamoDB table.
//
// The given items will be written in groups of 25 each.
//
// This method will retry:
// 	1 - if there are any any unprocessed items when performing the BatchWrite
// 	2 - if there is a Provisioning or Throttling aws error (tries for a max time of 3 minutes)
func (db dynamoDBSerivce) BatchWrite(items []DynamoDBItem) error {
	db.logger.Printf("writing batch of %d to %s", len(items), db.tableName)
	if len(items) == 0 {
		return nil
	}

	var remainingRequests []*dynamodb.WriteRequest
	for _, item := range items {
		if len(remainingRequests) == maxBatchWriteSize {
			if err := db.batchWriteItem(remainingRequests); err != nil {
				return err
			}

			remainingRequests = nil
		}

		request := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}
		remainingRequests = append(remainingRequests, request)
	}

	return db.batchWriteItem(remainingRequests)
}

func (db dynamoDBSerivce) batchWriteItem(requests []*dynamodb.WriteRequest) error {
	tableName := db.tableName

	writeRequests := requests
	for len(writeRequests) != 0 {
		retryHandler := func(attempt, elapsed int) (bool, error) {
			batchInput := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					tableName: writeRequests,
				},
			}

			output, err := db.client.BatchWriteItem(batchInput)
			if err == nil {
				writeRequests = output.UnprocessedItems[tableName]

				return true, nil
			}

			if awsErr, ok := err.(awserr.Error); ok {
				switch awsErr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					db.logger.Printf("batch write provisioning error: waited %d ms (attempt %d)", elapsed, attempt)
					return false, nil
				case errCodeThrottlingException:
					db.logger.Printf("batch write throttling error: waited %d ms (attempt %d)", elapsed, attempt)
					return false, nil
				default:
					return false, fmt.Errorf(
						"aws %s error in batch write to table %s: %s",
						awsErr.Code(),
						db.tableName,
						awsErr.Error(),
					)
				}
			}

			return false, fmt.Errorf("unable to batch write to table %s: %s", db.tableName, err)
		}

		if err := db.retry(retryHandler); err != nil {
			return err
		}
	}

	return nil
}

// WaitForReadyTable will wait for the table status to be active (waits for 3 minutes)
func (db dynamoDBSerivce) WaitForReadyTable() error {
	return db.retry(func(attempt, elapsed int) (bool, error) {
		description, err := db.DescribeTable()
		if err != nil {
			return false, err
		}

		return *description.TableStatus == dynamodb.TableStatusActive, nil
	})
}

func (db dynamoDBSerivce) retry(handler func(attempt, elapsed int) (bool, error)) error {
	elapsed := 0
	for attempt := 0; elapsed < maxRetryTime; attempt++ {
		handled, err := handler(attempt, elapsed)
		if err != nil {
			return err
		}

		if handled {
			return nil
		}

		elapsed += db.sleep(elapsed * attempt)
	}

	return fmt.Errorf("waited for too long (%d ms) to perform operation on %s table", elapsed, db.tableName)
}

// Scan allows you to perform a parallel scan over the table, writing the scanned items into the provided itemsChan
// If totalSegments is equal to 1, it will perform a sequential scan.
func (db dynamoDBSerivce) Scan(totalSegments, segment int, itemsChan chan<- []DynamoDBItem) error {
	if totalSegments == 0 {
		return errors.New("totalSegments has to be greater than 0")
	}

	input := dynamodb.ScanInput{
		TableName: aws.String(db.tableName),
	}

	if totalSegments > 1 {
		input.SetSegment(int64(segment))
		input.SetTotalSegments(int64(totalSegments))
	}

	totalScanned := 0
	pagerFn := func(output *dynamodb.ScanOutput, b bool) bool {
		var items []DynamoDBItem
		for _, item := range output.Items {
			items = append(items, item)
			totalScanned++
		}
		db.logger.Printf("%s table scanned page with %d items (reader %d)", db.tableName, len(items), segment)

		itemsChan <- items

		return !b
	}

	if err := db.client.ScanPages(&input, pagerFn); err != nil {
		return fmt.Errorf("unable to scan table %s: %s", db.tableName, err)
	}

	db.logger.Printf("%s table scanned a total of %d items (reader %d)", db.tableName, totalScanned, segment)

	return nil
}
