package dynamodbcopy_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestDebugLogger(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		subTestName string
		mocker      func(logger *mocks.Logger)
		debug       bool
	}{
		{
			"SuccessfulLog",
			func(logger *mocks.Logger) {
				logger.On("Printf", mock.AnythingOfType("string")).Once()
			},
			true,
		},
		{
			"NoLog",
			func(logger *mocks.Logger) {},
			false,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				loggerMock := &mocks.Logger{}

				testCase.mocker(loggerMock)

				logger := dynamodbcopy.NewDebugLogger(
					loggerMock,
					testCase.debug,
				)

				logger.Printf("running test")

				loggerMock.AssertExpectations(st)
			},
		)
	}
}
