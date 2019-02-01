package dynamodbcopy

import (
	"math/rand"
	"time"
)

const maxRandomFactor = 100

// Sleeper abstracts out sleep side effects to allow for better testing,
// receiving the intended sleep time (in ms) and returning the total time spent sleeping
type Sleeper func(ms int) int

// RandomSleeper will sleep for the provided time (in ms) plus an additional random factor (ranging between [0, 100[).
// Returns the total sleep time
func RandomSleeper(ms int) int {
	elapsed := ms + rand.Intn(maxRandomFactor)

	time.Sleep(time.Duration(elapsed) * time.Millisecond)

	return elapsed
}
