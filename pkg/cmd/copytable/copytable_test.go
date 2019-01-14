package copytable_test

import (
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

func TestRunCopyTable_FetchProvisioningError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")

	copierMock := &mocks.Copier{}

	provisionerMock := &mocks.Provisioner{}
	provisionerMock.
		On("Fetch").
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	err := copytable.RunCopyTable(copytable.Deps{Copier: copierMock, Provisioner: provisionerMock})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	copierMock.AssertExpectations(t)
	provisionerMock.AssertExpectations(t)
}

func TestRunCopyTable_UpdateProvisioningError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	copierMock := &mocks.Copier{}

	provisionerMock := &mocks.Provisioner{}
	provisionerMock.
		On("Fetch").
		Return(provisioning, nil).
		Once()
	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	err := copytable.RunCopyTable(copytable.Deps{Copier: copierMock, Provisioner: provisionerMock})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	copierMock.AssertExpectations(t)
	provisionerMock.AssertExpectations(t)
}

func TestRunCopyTable_CopyError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	provisionerMock := &mocks.Provisioner{}
	provisionerMock.
		On("Fetch").
		Return(provisioning, nil).
		Once()

	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	copierMock := &mocks.Copier{}
	copierMock.
		On("Copy").
		Return(expectedError).
		Once()

	err := copytable.RunCopyTable(copytable.Deps{Copier: copierMock, Provisioner: provisionerMock})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	copierMock.AssertExpectations(t)
	provisionerMock.AssertExpectations(t)
}

func TestRunCopyTable_RestoreProvisioningError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	provisionerMock := &mocks.Provisioner{}
	provisionerMock.
		On("Fetch").
		Return(provisioning, nil).
		Once()

	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()
	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	copierMock := &mocks.Copier{}
	copierMock.
		On("Copy").
		Return(nil).
		Once()

	err := copytable.RunCopyTable(copytable.Deps{Copier: copierMock, Provisioner: provisionerMock})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	copierMock.AssertExpectations(t)
	provisionerMock.AssertExpectations(t)
}

func TestRunCopyTable(t *testing.T) {
	t.Parallel()

	provisioning := dynamodbcopy.Provisioning{}

	provisionerMock := &mocks.Provisioner{}
	provisionerMock.
		On("Fetch").
		Return(provisioning, nil).
		Once()
	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()
	provisionerMock.
		On("Update", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	copierMock := &mocks.Copier{}
	copierMock.
		On("Copy").
		Return(nil).
		Once()

	err := copytable.RunCopyTable(copytable.Deps{Copier: copierMock, Provisioner: provisionerMock})

	require.Nil(t, err)

	copierMock.AssertExpectations(t)
	provisionerMock.AssertExpectations(t)
}

func TestSetAndBindFlags_Default(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	config := viper.New()

	err := copytable.SetAndBindFlags(cmd.Flags(), config)

	require.Nil(t, err)

	assert.Equal(t, 6, len(config.AllSettings()))
	assert.Equal(t, "", config.GetString("source-profile"))
	assert.Equal(t, "", config.GetString("target-profile"))
	assert.Equal(t, 0, config.GetInt("read-capacity"))
	assert.Equal(t, 0, config.GetInt("write-capacity"))
	assert.Equal(t, 1, config.GetInt("reader-count"))
	assert.Equal(t, 1, config.GetInt("writer-count"))
}
