package copytable_test

import (
	"errors"
	"testing"

	"github.com/magiconair/properties/assert"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

func TestRun_FetchProvisioningError(t *testing.T) {
	t.Parallel()

	service := &mocks.DynamoDBCopy{}

	expectedError := errors.New("error")
	service.
		On("FetchProvisioning").
		Return(dynamodbcopy.TablesDescription{}, expectedError).
		Once()

	err := copytable.Run(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRun_UpdateProvisioningError(t *testing.T) {
	t.Parallel()

	service := &mocks.DynamoDBCopy{}

	expectedError := errors.New("error")
	description := dynamodbcopy.TablesDescription{}
	service.
		On("FetchProvisioning").
		Return(description, nil).
		Once()

	service.
		On("CalculateCopyProvisioning", description).
		Return(description).
		Once()

	service.
		On("UpdateProvisioning", description).
		Return(expectedError).
		Once()

	err := copytable.Run(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRun_CopyError(t *testing.T) {
	t.Parallel()

	service := &mocks.DynamoDBCopy{}

	expectedError := errors.New("error")
	description := dynamodbcopy.TablesDescription{}
	service.
		On("FetchProvisioning").
		Return(description, nil).
		Once()

	service.
		On("CalculateCopyProvisioning", description).
		Return(description).
		Once()

	service.
		On("UpdateProvisioning", description).
		Return(nil).
		Once()

	service.
		On("Copy").
		Return(expectedError).
		Once()

	err := copytable.Run(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRun_UpdateProvisioningEndError(t *testing.T) {
	t.Parallel()

	service := &mocks.DynamoDBCopy{}

	expectedError := errors.New("error")
	description := dynamodbcopy.TablesDescription{}
	service.
		On("FetchProvisioning").
		Return(description, nil).
		Once()

	service.
		On("CalculateCopyProvisioning", description).
		Return(description).
		Once()

	service.
		On("UpdateProvisioning", description).
		Return(nil).
		Once()

	service.
		On("Copy").
		Return(nil).
		Once()

	service.
		On("UpdateProvisioning", description).
		Return(expectedError).
		Once()

	err := copytable.Run(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRun_Copy(t *testing.T) {
	t.Parallel()

	service := &mocks.DynamoDBCopy{}

	description := dynamodbcopy.TablesDescription{}
	service.
		On("FetchProvisioning").
		Return(description, nil).
		Once()

	service.
		On("CalculateCopyProvisioning", description).
		Return(description).
		Once()

	service.
		On("UpdateProvisioning", description).
		Return(nil).
		Twice()

	service.
		On("Copy").
		Return(nil).
		Once()

	err := copytable.Run(service)

	require.Nil(t, err)

	service.AssertExpectations(t)
}

func TestSetAndBindFlags_Default(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	config := viper.New()

	err := copytable.SetAndBindFlags(cmd.Flags(), config)

	require.Nil(t, err)

	assert.Equal(t, 4, len(config.AllSettings()))
	assert.Equal(t, "", config.GetString("source-profile"))
	assert.Equal(t, "", config.GetString("target-profile"))
	assert.Equal(t, 0, config.GetInt("read-units"))
	assert.Equal(t, 0, config.GetInt("write-units"))
}
