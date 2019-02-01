package dynamodbcopy

// Config encapsulates the values nedeed for the command
type Config struct {
	readCapacityUnits  int64
	writeCapacityUnits int64
	readWorkers        int
	writeWorkers       int
}

// NewConfig creates a new Config to store the parameters user defined parameters
func NewConfig(readUnits, writeUnits, readWorkers, writeWorkers int) Config {
	return Config{
		readCapacityUnits:  int64(readUnits),
		writeCapacityUnits: int64(writeUnits),
		readWorkers:        readWorkers,
		writeWorkers:       writeWorkers,
	}
}

// Provisioning calculates a new Provisioning value based on the passed argument and the current Config (receiver).
// The returned Provisioning value will have the higher values for read and write capacity units of the 2
func (c Config) Provisioning(current Provisioning) Provisioning {
	src := current.Source
	if src != nil && c.readCapacityUnits > src.Read {
		src = &Capacity{Read: c.readCapacityUnits, Write: src.Write}
	}

	trg := current.Target
	if trg != nil && c.writeCapacityUnits > trg.Write {
		trg = &Capacity{Read: trg.Read, Write: c.writeCapacityUnits}
	}

	return Provisioning{Source: src, Target: trg}
}

// Workers returns the Config read and write worker count
func (c Config) Workers() (int, int) {
	return c.readWorkers, c.writeWorkers
}
