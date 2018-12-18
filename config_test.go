package dynamodbcopy_test

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
)

func TestNewConfig(t *testing.T) {
	viperConfig := viper.New()

	expectedConfig := dynamodbcopy.Config{
		TargetTable: "test-Target",
		SourceTable: "test-Source",
	}

	viperConfig.Set("source-table", expectedConfig.SourceTable)
	viperConfig.Set("target-table", expectedConfig.TargetTable)

	config, err := dynamodbcopy.NewConfig(*viperConfig)
	require.Nil(t, err)

	assert.Equal(t, expectedConfig, config)
}
