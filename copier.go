package dynamodbcopy

import "sync"

type Copier interface {
	Copy(readers, writers int) error
}

type copyService struct {
	srcTable DynamoDBService
	trgTable DynamoDBService
}

func NewCopier(srcTableService, trgTableService DynamoDBService) Copier {
	return copyService{
		srcTable: srcTableService,
		trgTable: trgTableService,
	}
}

func (service copyService) Copy(readers, writers int) error {
	errChan := make(chan error)
	itemsChan := make(chan []DynamoDBItem)

	wgReaders := &sync.WaitGroup{}
	wgReaders.Add(readers)

	wgWriters := &sync.WaitGroup{}
	wgWriters.Add(writers)

	for i := 0; i < readers; i++ {
		go service.read(i, readers, wgReaders, itemsChan, errChan)
	}

	for i := 0; i < writers; i++ {
		go service.write(wgWriters, itemsChan, errChan)
	}

	go func() {
		wgReaders.Wait()
		close(itemsChan)
		wgWriters.Wait()
		close(errChan)
	}()

	return <-errChan
}

func (service copyService) read(
	readerID int,
	totalReaders int,
	wg *sync.WaitGroup,
	itemsChan chan<- []DynamoDBItem,
	errChan chan<- error,
) {
	defer wg.Done()

	items, err := service.srcTable.Scan(totalReaders, readerID)
	if err != nil {
		errChan <- err

		return
	}

	itemsChan <- items
}

func (service copyService) write(wg *sync.WaitGroup, itemsChan <-chan []DynamoDBItem, errChan chan<- error) {
	defer wg.Done()

	if err := service.trgTable.BatchWrite(<-itemsChan); err != nil {
		errChan <- err
	}
}
