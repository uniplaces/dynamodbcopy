package copytable

import (
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestRun(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("copyTable copy error")
	defaultConfig := dynamodbcopy.NewConfig(5, 5, 1, 1)
	defaultProvision := dynamodbcopy.Provisioning{}

	testCases := []struct {
		subTestName string
		mocker      func(copier *mocks.Copier, provisioner *mocks.Provisioner)
		expectError bool
		config      dynamodbcopy.Config
	}{
		{
			"FetchProvisioningError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(dynamodbcopy.Provisioning{}, expectedError).Once()
			},
			true,
			defaultConfig,
		},
		{
			"UpdateError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, expectedError).Once()
			},
			true,
			defaultConfig,
		},
		{
			"CopyError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Twice()
				copier.On("Copy", 1, 1).Return(expectedError).Once()
			},
			true,
			defaultConfig,
		},
		{
			"CopyErrorWithRestoreError",
			func(copier *mocks.Copier, provisioner *mocks.Provisioner) {
				provisioner.On("Fetch").Return(defaultProvision, nil).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, nil).Once()
				copier.On("Copy", 1, 1).Return(expectedError).Once()
				provisioner.On("Update", defaultProvision).Return(defaultProvision, expectedError).Once()
			},
			true,
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
			true,
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
			false,
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

				deps := dependencies{
					Copier:      copierMock,
					Provisioner: provisionerMock,
					Config:      testCase.config,
				}

				err := run(deps)

				if testCase.expectError {
					require.NotNil(t, err)
				} else {
					require.Nil(t, err)
				}

				copierMock.AssertExpectations(st)
				provisionerMock.AssertExpectations(st)
			},
		)
	}
}

func TestBindFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}

	bindFlags(cmd.Flags())

	require.NotNil(t, cmd.Flag("source-profile"))
	require.NotNil(t, cmd.Flag("target-profile"))
	require.NotNil(t, cmd.Flag("read-capacity"))
	require.NotNil(t, cmd.Flag("write-capacity"))
	require.NotNil(t, cmd.Flag("reader-count"))
	require.NotNil(t, cmd.Flag("writer-count"))
}

func TestSetupDependencies(t *testing.T) {
	expectedConfig := dynamodbcopy.NewConfig(0, 0, 1, 1)

	cmd := &cobra.Command{}

	bindFlags(cmd.Flags())

	deps, err := setupDependencies(cmd, []string{"src", "trg"})

	require.Nil(t, err)
	require.NotNil(t, deps.Provisioner)
	require.NotNil(t, deps.Copier)

	assert.Equal(t, expectedConfig, deps.Config)
}
