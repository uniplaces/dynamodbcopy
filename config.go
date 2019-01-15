package dynamodbcopy

type Config struct {
	readCapacityUnits  int64
	writeCapacityUnits int64
	readWorkers        int
	writeWorkers       int
}

func NewConfig(readUnits, writeUnits, readWorkers, writeWorkers int) Config {
	return Config{
		readCapacityUnits:  int64(readUnits),
		writeCapacityUnits: int64(writeUnits),
		readWorkers:        readWorkers,
		writeWorkers:       writeWorkers,
	}
}

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

func (c Config) Workers() (int, int) {
	return c.readWorkers, c.writeWorkers
}
