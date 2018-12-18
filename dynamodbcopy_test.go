package dynamodbcopy_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestFetchProvisioning(t *testing.T) {
	t.Parallel()

	srcDescription := getTableDescription("src-table-name")
	trgDescription := getTableDescription("trg-table-name")

	expectedTableDescriptions := dynamodbcopy.TablesDescription{
		Source: *srcDescription,
		Target: *trgDescription,
	}

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(srcDescription, nil).
		Once()

	trgService := &mocks.DynamoDBService{}
	trgService.
		On("DescribeTable").
		Return(trgDescription, nil).
		Once()

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	description, err := copyService.FetchProvisioning()
	require.Nil(t, err)

	assert.Equal(t, expectedTableDescriptions, description)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestFetchProvisioning_SrcError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("dynamo errors")

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	_, err = copyService.FetchProvisioning()

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestFetchProvisioning_TrgError(t *testing.T) {
	t.Parallel()

	srcDescription := getTableDescription("src-table-name")

	expectedError := errors.New("dynamo errors")

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(srcDescription, nil).
		Once()

	trgService := &mocks.DynamoDBService{}
	trgService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	_, err = copyService.FetchProvisioning()

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func getTableDescription(table string) *dynamodb.TableDescription {
	return &dynamodb.TableDescription{
		TableName: aws.String(table),
	}
}
