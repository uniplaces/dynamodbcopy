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

func TestRunCopyTable(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("run copy error")
	defaultConfig := dynamodbcopy.NewConfig(5, 5, 1, 1)
	defaultProvision := dynamodbcopy.Provisioning{}

	testCases := []struct {
		subTestName   string
		mocker        func(copier *mocks.Copier, provisioner *mocks.Provisioner)
		expectedError error
		config        dynamodbcopy.Config
	}{
		{
			"FetchProvisioningError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(dynamodbcopy.Provisioning{}, expectedError).Once()
			},
			expectedError,
			defaultConfig,
		},
		{
			"UpdateError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, expectedError).Once()
			},
			expectedError,
			defaultConfig,
		},
		{
			"CopyError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Once()
				copier.On("Copy", 1, 1).Return(expectedError).Once()
			},
			expectedError,
			defaultConfig,
		},
		{
			"RestoreProvisioningError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Once()
				copier.On("Copy", 1, 1).Return(nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, expectedError).Once()
			},
			expectedError,
			defaultConfig,
		},
		{
			"Success",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Once()
				copier.On("Copy", 1, 1).Return(nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Once()
			},
			nil,
			defaultConfig,
		},
	}
	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				copierMock := &mocks.Copier{}
				provisionerMock := &mocks.Provisioner{}

				testCase.mocker(copierMock, provisionerMock)

				deps := copytable.Deps{
					Copier:      copierMock,
					Provisioner: provisionerMock,
					Config:      testCase.config,
				}

				err := copytable.RunCopyTable(deps)

				assert.Equal(t, testCase.expectedError, err)

				copierMock.AssertExpectations(st)
				provisionerMock.AssertExpectations(st)
			},
		)
	}
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
