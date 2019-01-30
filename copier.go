package dynamodbcopy

import (
	"fmt"
	"sync"
)

type Copier interface {
	Copy(readers, writers int) error
}

type copyService struct {
	srcTable DynamoDBService
	trgTable DynamoDBService
	chans    CopierChans
	logger   Logger
}

func NewCopier(srcTableService, trgTableService DynamoDBService, chans CopierChans, logger Logger) Copier {
	return copyService{
		srcTable: srcTableService,
		trgTable: trgTableService,
		chans:    chans,
		logger:   logger,
	}
}

func (service copyService) Copy(readers, writers int) error {
	service.logger.Printf("copying table with %d readers and %d writers", readers, writers)
	itemsChan, errChan := service.chans.Items, service.chans.Errors

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
	defer func() {
		if err := recover(); err != nil {
			errChan <- fmt.Errorf("read recovery: %s", err)
		}
		wg.Done()
	}()

	err := service.srcTable.Scan(totalReaders, readerID, itemsChan)
	if err != nil {
		errChan <- err
	}
}

func (service copyService) write(wg *sync.WaitGroup, itemsChan <-chan []DynamoDBItem, errChan chan<- error) {
	defer func() {
		if err := recover(); err != nil {
			errChan <- fmt.Errorf("write recovery: %s", err)
		}
		wg.Done()
	}()

	totalWritten := 0
	for items := range itemsChan {
		if err := service.trgTable.BatchWrite(items); err != nil {
			errChan <- err
		}

		totalWritten += len(items)
	}

	service.logger.Printf("writer wrote a total of %d items", totalWritten)
}

// CopierChans encapsulates the chan that are used by the copier
type CopierChans struct {
	Items  chan []DynamoDBItem
	Errors chan error
}

func NewCopierChans(itemsChanSize int) CopierChans {
	return CopierChans{
		Items:  make(chan []DynamoDBItem, itemsChanSize),
		Errors: make(chan error),
	}
}
