package dynamodbcopy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uniplaces/dynamodbcopy"
)

func TestWorkers(t *testing.T) {
	t.Parallel()

	config := dynamodbcopy.NewConfig(0, 0, 1, 1)

	r, w := config.Workers()

	assert.Equal(t, 1, r)
	assert.Equal(t, 1, w)
}

func TestProvisioning(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		subTestName          string
		config               dynamodbcopy.Config
		currentProvisioning  dynamodbcopy.Provisioning
		expectedProvisioning dynamodbcopy.Provisioning
	}{
		{
			"NoCurrentProvision",
			dynamodbcopy.NewConfig(0, 0, 1, 1),
			buildProvision(0, 0),
			buildProvision(0, 0),
		},
		{
			"NoSrcProvision",
			dynamodbcopy.NewConfig(5, 5, 1, 1),
			buildProvision(0, 10),
			buildProvision(0, 10),
		},
		{
			"NoTrgProvision",
			dynamodbcopy.NewConfig(5, 5, 1, 1),
			buildProvision(10, 0),
			buildProvision(10, 0),
		},
		{
			"NoUpdateProvisioning",
			dynamodbcopy.NewConfig(5, 5, 1, 1),
			buildProvision(10, 10),
			buildProvision(10, 10),
		},
		{
			"UpdateProvisioning",
			dynamodbcopy.NewConfig(12, 12, 1, 1),
			buildProvision(10, 10),
			buildProvision(12, 12),
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {

				provisioning := testCase.config.Provisioning(testCase.currentProvisioning)

				assert.Equal(t, testCase.expectedProvisioning, provisioning)
			},
		)
	}
}

func buildProvision(r, w int64) dynamodbcopy.Provisioning {
	var src *dynamodbcopy.Capacity
	var trg *dynamodbcopy.Capacity

	if r != 0 {
		src = &dynamodbcopy.Capacity{Read: r, Write: 5}
	}

	if w != 0 {
		trg = &dynamodbcopy.Capacity{Read: 5, Write: w}
	}

	return dynamodbcopy.Provisioning{
		Source: src,
		Target: trg,
	}
}
