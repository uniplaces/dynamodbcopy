package dynamodbcopy

import (
	"math/rand"
	"time"
)

const (
	maxRandomFactor = 100
)

// Sleeper abstracts out sleep side effects to allow better testing
type Sleeper func(elapsedMilliseconds int) int

func RandomSleeper(elapsedMilliseconds int) int {
	elapsed := elapsedMilliseconds + rand.Intn(maxRandomFactor)

	time.Sleep(time.Duration(elapsed) * time.Millisecond)

	return elapsed
}
