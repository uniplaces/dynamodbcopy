package dynamodbcopy_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

const (
	expectedTableName = "test-table-name"
)

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	expectedDescription := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)
	expectedError := errors.New("describeTableError")

	descriptionMock := mock.AnythingOfType("*dynamodb.DescribeTableInput")

	testCases := []struct {
		subTestName         string
		mocker              func(api *mocks.DynamoDBAPI)
		expectedError       error
		expectedDescription *dynamodb.TableDescription
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(nil, expectedError).Once()
			},
			expectedError,
			nil,
		},
		{
			"Success",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(expectedDescription, nil).Once()
			},
			nil,
			expectedDescription.Table,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				description, err := service.DescribeTable()

				assert.Equal(t, testCase.expectedError, err)
				assert.Equal(t, testCase.expectedDescription, description)

				api.AssertExpectations(st)
			},
		)
	}
}

func TestUpdateCapacity(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("updateCapacityError")

	updateMock := mock.AnythingOfType("*dynamodb.UpdateTableInput")
	describeMock := mock.AnythingOfType("*dynamodb.DescribeTableInput")

	testCases := []struct {
		subTestName   string
		mocker        func(api *mocks.DynamoDBAPI)
		capacity      dynamodbcopy.Capacity
		expectedError error
	}{
		{
			"ZeroError",
			func(api *mocks.DynamoDBAPI) {},
			dynamodbcopy.Capacity{Read: 0, Write: 10},
			errors.New("invalid update capacity read 0, write 10: capacity units must be greater than 0"),
		},
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("UpdateTable", updateMock).Return(nil, expectedError).Once()
			},
			dynamodbcopy.Capacity{Read: 10, Write: 10},
			expectedError,
		},
		{
			"Update",
			func(api *mocks.DynamoDBAPI) {
				api.On("UpdateTable", updateMock).Return(&dynamodb.UpdateTableOutput{}, nil).Once()
				output := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)
				api.On("DescribeTable", describeMock).Return(output, nil).Once()
			},
			dynamodbcopy.Capacity{Read: 10, Write: 10},
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				err := service.UpdateCapacity(testCase.capacity)

				assert.Equal(t, testCase.expectedError, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func TestWaitForReadyTable(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("waitForReadyTableError")

	activeDescribeOutput := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)
	creatingDescribeOutput := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusCreating)

	descriptionMock := mock.AnythingOfType("*dynamodb.DescribeTableInput")

	testCases := []struct {
		subTestName    string
		mocker         func(api *mocks.DynamoDBAPI)
		expectedCalled int
		expectedError  error
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(nil, expectedError).Once()
			},
			0,
			expectedError,
		},
		{
			"SuccessOnFirstAttempt",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(activeDescribeOutput, nil).Once()
			},
			0,
			nil,
		},
		{
			"SuccessOnMultipleAttempts",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(creatingDescribeOutput, nil).Times(4)
				api.On("DescribeTable", descriptionMock).Return(activeDescribeOutput, nil).Once()
			},
			4,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				called := 0
				sleeperFn := func(elapsedMilliseconds int) int {
					called++

					return called
				}
				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, sleeperFn)

				err := service.WaitForReadyTable()

				assert.Equal(t, testCase.expectedError, err)
				assert.Equal(t, testCase.expectedCalled, called)

				api.AssertExpectations(st)
			},
		)
	}
}

func TestBatchWrite(t *testing.T) {
	t.Parallel()

	defaultBatchInput := buildBatchWriteItemInput(10)

	firstBatchInput := buildBatchWriteItemInput(25)
	secondBatchInput := buildBatchWriteItemInput(24)

	unprocessedOuput := &dynamodb.BatchWriteItemOutput{UnprocessedItems: defaultBatchInput.RequestItems}

	expectedError := errors.New("batch write error")

	testCases := []struct {
		subTestName   string
		mocker        func(api *mocks.DynamoDBAPI)
		items         []dynamodbcopy.DynamoDBItem
		expectedError error
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(nil, expectedError).Once()
			},
			getItems(defaultBatchInput),
			expectedError,
		},
		{
			"NoItems",
			func(api *mocks.DynamoDBAPI) {},
			[]dynamodbcopy.DynamoDBItem{},
			nil,
		},
		{
			"LessThanMaxBatchSize",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			nil,
		},
		{
			"GreaterThanMaxBatchSize",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &firstBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
				api.On("BatchWriteItem", &secondBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			append(
				getItems(firstBatchInput),
				getItems(secondBatchInput)...,
			),
			nil,
		},
		{
			"GreaterThanMaxBatchSizeWithError",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &firstBatchInput).Return(nil, expectedError).Once()
			},
			append(
				getItems(firstBatchInput),
				getItems(secondBatchInput)...,
			),
			expectedError,
		},
		{
			"UnprocessedItems",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(unprocessedOuput, nil).
					Once()

				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				err := service.BatchWrite(testCase.items)

				assert.Equal(t, testCase.expectedError, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func TestScan(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("scan error")

	testCases := []struct {
		subTestName   string
		mocker        func(api *mocks.DynamoDBAPI)
		totalSegments int
		segment       int
		expectedError error
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(expectedError).Once()
			},
			1,
			0,
			expectedError,
		},
		{
			"TotalSegmentsError",
			func(api *mocks.DynamoDBAPI) {},
			0,
			0,
			errors.New("totalSegments has to be greater than 0"),
		},
		{
			"TotalSegmentsIsOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(nil).Once()
			},
			1,
			0,
			nil,
		},
		{
			"TotalSegmentsIsGreaterThanOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(5, 2), mock.Anything).Return(nil).Once()
			},
			5,
			2,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				_, err := service.Scan(testCase.totalSegments, testCase.segment)

				assert.Equal(t, testCase.expectedError, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func buildScanInput(totalSegments, segment int64) *dynamodb.ScanInput {
	if totalSegments < 2 {
		return &dynamodb.ScanInput{
			TableName: aws.String(expectedTableName),
		}
	}

	return &dynamodb.ScanInput{
		TableName:     aws.String(expectedTableName),
		TotalSegments: aws.Int64(totalSegments),
		Segment:       aws.Int64(segment),
	}
}

func buildBatchWriteItemInput(itemCount int) dynamodb.BatchWriteItemInput {
	items := map[string][]*dynamodb.WriteRequest{}

	requests := make([]*dynamodb.WriteRequest, itemCount)
	for i := 0; i < itemCount; i++ {
		requests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String(fmt.Sprintf("%d", i)),
					},
				},
			},
		}
	}

	items[expectedTableName] = requests

	return dynamodb.BatchWriteItemInput{
		RequestItems: items,
	}
}

func getItems(batchInput dynamodb.BatchWriteItemInput) []dynamodbcopy.DynamoDBItem {
	items := make([]dynamodbcopy.DynamoDBItem, len(batchInput.RequestItems[expectedTableName]))
	for i, writeRequest := range batchInput.RequestItems[expectedTableName] {
		items[i] = dynamodbcopy.DynamoDBItem(writeRequest.PutRequest.Item)
	}

	return items
}

func buildDescribeTableOutput(tableName, status string) *dynamodb.DescribeTableOutput {
	return &dynamodb.DescribeTableOutput{
		Table: &dynamodb.TableDescription{
			TableName:   aws.String(tableName),
			TableStatus: aws.String(status),
		},
	}
}

func testSleeper(ms int) int {
	return ms // skip
}
