package dynamodbcopy_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
		errorExpected       bool
		expectedDescription *dynamodb.TableDescription
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(nil, expectedError).Once()
			},
			true,
			nil,
		},
		{
			"Success",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(expectedDescription, nil).Once()
			},
			false,
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

				assertExpectedError(st, testCase.errorExpected, err)
				assert.Equal(st, testCase.expectedDescription, description)

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
		errorExpected bool
	}{
		{
			"ZeroError",
			func(api *mocks.DynamoDBAPI) {},
			dynamodbcopy.Capacity{Read: 0, Write: 10},
			true,
		},
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("UpdateTable", updateMock).Return(nil, expectedError).Once()
			},
			dynamodbcopy.Capacity{Read: 10, Write: 10},
			true,
		},
		{
			"Update",
			func(api *mocks.DynamoDBAPI) {
				api.On("UpdateTable", updateMock).Return(&dynamodb.UpdateTableOutput{}, nil).Once()
				output := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)
				api.On("DescribeTable", describeMock).Return(output, nil).Once()
			},
			dynamodbcopy.Capacity{Read: 10, Write: 10},
			false,
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

				assertExpectedError(st, testCase.errorExpected, err)

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
		errorExpected  bool
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(nil, expectedError).Once()
			},
			0,
			true,
		},
		{
			"SuccessOnFirstAttempt",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(activeDescribeOutput, nil).Once()
			},
			0,
			false,
		},
		{
			"SuccessOnMultipleAttempts",
			func(api *mocks.DynamoDBAPI) {
				api.On("DescribeTable", descriptionMock).Return(creatingDescribeOutput, nil).Times(4)
				api.On("DescribeTable", descriptionMock).Return(activeDescribeOutput, nil).Once()
			},
			4,
			false,
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

				assertExpectedError(st, testCase.errorExpected, err)
				assert.Equal(st, testCase.expectedCalled, called)

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
		errorExpected bool
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(nil, expectedError).Once()
			},
			getItems(defaultBatchInput),
			true,
		},
		{
			"NoItems",
			func(api *mocks.DynamoDBAPI) {},
			[]dynamodbcopy.DynamoDBItem{},
			false,
		},
		{
			"LessThanMaxBatchSize",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			false,
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
			false,
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
			true,
		},
		{
			"UnprocessedItems",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(unprocessedOuput, nil).
					Once()

				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			false,
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

				assertExpectedError(st, testCase.errorExpected, err)

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
		errorExpected bool
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(expectedError).Once()
			},
			1,
			0,
			true,
		},
		{
			"TotalSegmentsError",
			func(api *mocks.DynamoDBAPI) {},
			0,
			0,
			true,
		},
		{
			"TotalSegmentsIsOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(nil).Once()
			},
			1,
			0,
			false,
		},
		{
			"TotalSegmentsIsGreaterThanOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(5, 2), mock.Anything).Return(nil).Once()
			},
			5,
			2,
			false,
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

				assertExpectedError(st, testCase.errorExpected, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func assertExpectedError(t *testing.T, errorExpected bool, err error) {
	if errorExpected {
		require.NotNil(t, err)
	} else {
		require.Nil(t, err)
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
