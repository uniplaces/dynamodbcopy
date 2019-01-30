package dynamodbcopy_test

import (
	"errors"
	"io/ioutil"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

const (
	srcTableName = "src-table-name"
	trgTableName = "trg-table-name"
)

func TestFetch(t *testing.T) {
	t.Parallel()

	srcDefaultDescription := buildDefaultTableDescription(srcTableName)
	trgDefaultDescription := buildDefaultTableDescription(trgTableName)

	expectedError := errors.New("dynamo errors")

	testCases := []struct {
		subTestName          string
		mocker               func(srcService, trgService *mocks.DynamoDBService)
		expectedProvisioning dynamodbcopy.Provisioning
		expectedError        error
	}{
		{
			"Success",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
			},
			buildProvisioning(srcDefaultDescription, trgDefaultDescription),
			nil,
		},
		{
			"SrcDescribeError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(nil, expectedError).Once()
			},
			dynamodbcopy.Provisioning{},
			expectedError,
		},
		{
			"TrgDescribeError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(nil, expectedError).Once()
			},
			dynamodbcopy.Provisioning{},
			expectedError,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				srcService := &mocks.DynamoDBService{}
				trgService := &mocks.DynamoDBService{}

				testCase.mocker(srcService, trgService)

				provisioner := dynamodbcopy.NewProvisioner(
					srcService,
					trgService,
					log.New(ioutil.Discard, "", log.Ltime),
				)

				fetchedProvisioning, err := provisioner.Fetch()

				assert.Equal(t, testCase.expectedProvisioning, fetchedProvisioning)
				assert.Equal(t, testCase.expectedError, err)

				srcService.AssertExpectations(st)
				trgService.AssertExpectations(st)
			},
		)
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	srcDefaultDescription := buildDefaultTableDescription(srcTableName)
	trgDefaultDescription := buildDefaultTableDescription(trgTableName)

	srcDescription := buildTableDescription(srcTableName, dynamodb.BillingModeProvisioned, 10, 10)
	trgDescription := buildTableDescription(trgTableName, dynamodb.BillingModeProvisioned, 10, 10)

	srcPerRequestDescription := buildTableDescription(srcTableName, dynamodb.BillingModePayPerRequest, 10, 10)
	trgPerRequestDescription := buildTableDescription(trgTableName, dynamodb.BillingModePayPerRequest, 10, 10)

	expectedError := errors.New("dynamo errors")

	testCases := []struct {
		subTestName          string
		mocker               func(srcService, trgService *mocks.DynamoDBService)
		updateProvisioning   dynamodbcopy.Provisioning
		expectedProvisioning dynamodbcopy.Provisioning
		expectedError        error
	}{
		{
			"FetchSrcDescribeError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(nil, expectedError).Once()
			},
			dynamodbcopy.Provisioning{},
			dynamodbcopy.Provisioning{},
			expectedError,
		},
		{
			"FetchTrgDescribeError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(nil, expectedError).Once()
			},
			dynamodbcopy.Provisioning{},
			dynamodbcopy.Provisioning{},
			expectedError,
		},
		{
			"NoUpdateNeeded",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
			},
			buildProvisioning(srcDefaultDescription, trgDefaultDescription),
			buildProvisioning(srcDefaultDescription, trgDefaultDescription),
			nil,
		},
		{
			"SrcUpdateNeeded",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				srcService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
			},
			buildProvisioning(srcDescription, trgDefaultDescription),
			buildProvisioning(srcDescription, trgDefaultDescription),
			nil,
		},
		{
			"TrgUpdateNeeded",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
				trgService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(nil).Once()
			},
			buildProvisioning(srcDefaultDescription, trgDescription),
			buildProvisioning(srcDefaultDescription, trgDescription),
			nil,
		},
		{
			"Update",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				srcService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
				trgService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(nil).Once()
			},
			buildProvisioning(srcDescription, trgDescription),
			buildProvisioning(srcDescription, trgDescription),
			nil,
		},
		{
			"NoUpdateNeededPerRequestBilling",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcPerRequestDescription, nil).Once()
				trgService.On("DescribeTable").Return(&trgPerRequestDescription, nil).Once()
			},
			buildProvisioning(srcPerRequestDescription, srcPerRequestDescription),
			buildProvisioning(srcPerRequestDescription, trgPerRequestDescription),
			nil,
		},
		{
			"UpdateSrcError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				srcService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(expectedError).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
			},
			buildProvisioning(srcDescription, trgDefaultDescription),
			dynamodbcopy.Provisioning{},
			expectedError,
		},
		{
			"UpdateTrgError",
			func(srcService, trgService *mocks.DynamoDBService) {
				srcService.On("DescribeTable").Return(&srcDefaultDescription, nil).Once()
				trgService.On("DescribeTable").Return(&trgDefaultDescription, nil).Once()
				trgService.On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).Return(expectedError).Once()
			},
			buildProvisioning(srcDefaultDescription, trgDescription),
			dynamodbcopy.Provisioning{},
			expectedError,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				srcService := &mocks.DynamoDBService{}
				trgService := &mocks.DynamoDBService{}

				testCase.mocker(srcService, trgService)

				provisioner := dynamodbcopy.NewProvisioner(
					srcService,
					trgService,
					log.New(ioutil.Discard, "", log.Ltime),
				)

				fetchedProvisioning, err := provisioner.Update(testCase.updateProvisioning)

				assert.Equal(t, testCase.expectedProvisioning, fetchedProvisioning)
				assert.Equal(t, testCase.expectedError, err)

				srcService.AssertExpectations(st)
				trgService.AssertExpectations(st)
			},
		)
	}
}

func buildProvisioning(src, trg dynamodb.TableDescription) dynamodbcopy.Provisioning {
	return dynamodbcopy.NewProvisioning(&src, &trg)
}

func buildDefaultTableDescription(table string) dynamodb.TableDescription {
	return buildTableDescription(table, dynamodb.BillingModeProvisioned, 5, 5)
}

func buildTableDescription(table, billingMode string, r, w int64) dynamodb.TableDescription {
	return dynamodb.TableDescription{
		TableName: aws.String(table),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
			ReadCapacityUnits:  aws.Int64(r),
			WriteCapacityUnits: aws.Int64(w),
		},
		BillingModeSummary: &dynamodb.BillingModeSummary{
			BillingMode: aws.String(billingMode),
		},
	}
}
