package main

import "github.com/uniplaces/dynamodbcopy/pkg/cmd"

func main() {
	cmd.New().Execute() // nolint: errcheck
}
