package main

import (
	"github.com/spf13/viper"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd"
)

func main() {
	cmd.New(viper.New()).Execute() // nolint: errcheck
}
