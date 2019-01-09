package dynamodbcopy

type Copier interface {
	Copy() error
}

type copyService struct {
	config   Config
	srcTable DynamoDBService
	trgTable DynamoDBService
}

func NewCopier(copyConfig Config, srcTableService, trgTableService DynamoDBService) Copier {
	return copyService{
		config:   copyConfig,
		srcTable: srcTableService,
		trgTable: trgTableService,
	}
}

func (dc copyService) Copy() error {
	return nil
}
