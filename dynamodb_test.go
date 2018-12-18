package dynamodbcopy_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestDescribeTable(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedTableName := "table-name"
	expectedTableDescription := getDescribeTableOutput(expectedTableName)

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(expectedTableDescription, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api)

	description, err := service.DescribeTable()
	require.Nil(t, err)

	assert.Equal(t, expectedTableDescription.Table, description)

	api.AssertExpectations(t)
}

func TestDescribeTable_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedTableName := "table-name"
	expectedError := errors.New("error")

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api)

	_, err := service.DescribeTable()
	require.NotNil(t, err)

	assert.Equal(t, expectedError, err)

	api.AssertExpectations(t)
}

func getDescribeTableOutput(tableName string) *dynamodb.DescribeTableOutput {
	return &dynamodb.DescribeTableOutput{
		Table: &dynamodb.TableDescription{
			TableName: aws.String(tableName),
		},
	}
}
