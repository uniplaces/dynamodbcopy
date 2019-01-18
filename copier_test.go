package dynamodbcopy_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestCopy(t *testing.T) {
	t.Parallel()

	scanError := errors.New("scanError")
	batchWriteError := errors.New("batchWriteError")

	testCases := []struct {
		subTestName   string
		mocker        func(src, trg *mocks.DynamoDBService)
		totalReaders  int
		totalWriters  int
		expectedError error
	}{
		{
			"ScanError",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", 1, 0).Return(nil, scanError).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(nil).Maybe()
			},
			1,
			1,
			scanError,
		},
		{
			"BatchWriteError",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", 1, 0).Return([]dynamodbcopy.DynamoDBItem{}, nil).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(batchWriteError).Once()
			},
			1,
			1,
			batchWriteError,
		},
		{
			"Success",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", 1, 0).Return([]dynamodbcopy.DynamoDBItem{}, nil).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(nil).Once()
			},
			1,
			1,
			nil,
		},
		{
			"MultipleWorkers",
			func(src, trg *mocks.DynamoDBService) {
				first := buildItem("1")
				src.On("Scan", 3, 0).Return(first, nil).Once()
				trg.On("BatchWrite", first).Return(nil).Once()

				second := buildItem("2")
				src.On("Scan", 3, 1).Return(second, nil).Once()
				trg.On("BatchWrite", second).Return(nil).Once()

				third := buildItem("3")
				src.On("Scan", 3, 2).Return(third, nil).Once()
				trg.On("BatchWrite", third).Return(nil).Once()
			},
			3,
			3,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				src := &mocks.DynamoDBService{}
				trg := &mocks.DynamoDBService{}

				testCase.mocker(src, trg)

				service := dynamodbcopy.NewCopier(src, trg)

				err := service.Copy(testCase.totalReaders, testCase.totalWriters)

				assert.Equal(st, testCase.expectedError, err)

				src.AssertExpectations(st)
				trg.AssertExpectations(st)
			},
		)
	}
}

func buildItem(id string) []dynamodbcopy.DynamoDBItem {
	return []dynamodbcopy.DynamoDBItem{
		{
			"id": &dynamodb.AttributeValue{
				S: aws.String(id),
			},
		},
	}
}
