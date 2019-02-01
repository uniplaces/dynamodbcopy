package dynamodbcopy_test

import (
	"errors"
	"io/ioutil"
	"log"
	"strconv"
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
		mocker        func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan)
		totalReaders  int
		totalWriters  int
		expectedError error
	}{
		{
			"ScanError",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 1, 0, readChan).Return(scanError).Once()
			},
			1,
			1,
			scanError,
		},
		{
			"BatchWriteError",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 1, 0, readChan).Return(nil).Once()

				items := buildItems(1)
				chans.Items <- items
				trg.On("BatchWrite", items).Return(batchWriteError).Once()
			},
			1,
			1,
			batchWriteError,
		},
		{
			"Success",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 1, 0, readChan).Return(nil).Once()

				items := buildItems(1)
				chans.Items <- items
				trg.On("BatchWrite", items).Return(nil).Once()
			},
			1,
			1,
			nil,
		},
		{
			"MultipleWorkers",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 3, 0, readChan).Return(nil).Once()
				src.On("Scan", 3, 1, readChan).Return(nil).Once()
				src.On("Scan", 3, 2, readChan).Return(nil).Once()

				items1 := buildItems(1)
				chans.Items <- items1
				trg.On("BatchWrite", items1).Return(nil).Once()

				items2 := buildItems(2)
				chans.Items <- items2
				trg.On("BatchWrite", items2).Return(nil).Once()

				items3 := buildItems(3)
				chans.Items <- items3
				trg.On("BatchWrite", items3).Return(nil).Once()
			},
			3,
			3,
			nil,
		},
		{
			"ReadPanic",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 1, 0, readChan).Run(func(args mock.Arguments) {
					panic("read panic")
				}).Once()
			},
			1,
			1,
			errors.New("read recovery: read panic"),
		},
		{
			"WritePanic",
			func(src, trg *mocks.DynamoDBService, chans *dynamodbcopy.CopierChan) {
				var readChan chan<- []dynamodbcopy.DynamoDBItem = chans.Items
				src.On("Scan", 1, 0, readChan).Return(nil).Once()

				items := buildItems(1)
				chans.Items <- items
				trg.On("BatchWrite", items).Run(func(args mock.Arguments) {
					panic("write panic")
				}).Once()
			},
			1,
			1,
			errors.New("write recovery: write panic"),
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				src := &mocks.DynamoDBService{}
				trg := &mocks.DynamoDBService{}

				copierChans := dynamodbcopy.NewCopierChan(testCase.totalWriters)

				testCase.mocker(src, trg, &copierChans)

				service := dynamodbcopy.NewCopier(src, trg, copierChans, log.New(ioutil.Discard, "", log.Ltime))

				err := service.Copy(testCase.totalReaders, testCase.totalWriters)

				assert.Equal(st, testCase.expectedError, err)

				select {
				case _, ok := <-copierChans.Items:
					assert.False(st, ok, "items chan should be closed")
				case _, ok := <-copierChans.Errors:
					assert.False(st, ok, "errors chan should be closed")
				}

				src.AssertExpectations(st)
				trg.AssertExpectations(st)
			},
		)
	}
}

func buildItems(numItems int) []dynamodbcopy.DynamoDBItem {
	items := make([]dynamodbcopy.DynamoDBItem, numItems)

	for i := 0; i < numItems; i++ {
		items[i] = dynamodbcopy.DynamoDBItem{
			"id": &dynamodb.AttributeValue{
				S: aws.String(strconv.Itoa(i)),
			},
		}
	}

	return items
}
