package dynamodbcopy_test

import (
	"errors"
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
	api := &mocks.DynamoDBAPI{}

	expectedTableDescription := getDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(expectedTableDescription, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	description, err := service.DescribeTable()
	require.Nil(t, err)

	assert.Equal(t, expectedTableDescription.Table, description)

	api.AssertExpectations(t)
}

func TestDescribeTable_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	_, err := service.DescribeTable()
	require.NotNil(t, err)

	assert.Equal(t, expectedError, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity_ZeroError(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 0, Write: 10})

	require.NotNil(t, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")

	api.
		On("UpdateTable", mock.AnythingOfType("*dynamodb.UpdateTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 10, Write: 10})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	api.
		On("UpdateTable", mock.AnythingOfType("*dynamodb.UpdateTableInput")).
		Return(&dynamodb.UpdateTableOutput{}, nil).
		Once()

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(getDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 10, Write: 10})

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")
	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.WaitForReadyTable()

	require.NotNil(t, err)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_OnFirstAttempt(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	called := 0
	sleeperFn := func(elapsedMilliseconds int) int {
		called++

		return called
	}

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(getDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, sleeperFn)
	err := service.WaitForReadyTable()

	require.Nil(t, err)
	assert.Equal(t, 0, called)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_OnMultipleAttempts(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	attempts := 4

	called := 0
	sleeperFn := func(elapsedMilliseconds int) int {
		called++

		return elapsedMilliseconds
	}

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(getDescribeTableOutput(expectedTableName, dynamodb.TableStatusCreating), nil).
		Times(attempts)

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(getDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, sleeperFn)
	err := service.WaitForReadyTable()

	require.Nil(t, err)
	assert.Equal(t, attempts, called)

	api.AssertExpectations(t)
}

func getDescribeTableOutput(tableName, status string) *dynamodb.DescribeTableOutput {
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
