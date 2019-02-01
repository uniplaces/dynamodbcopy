package dynamodbcopy

import (
	"fmt"
	"sync"
)

// Copier is the interface that allows you to copy records from the source to target table
type Copier interface {
	Copy(readers, writers int) error
}

type copyService struct {
	srcTable   DynamoDBService
	trgTable   DynamoDBService
	copierChan CopierChan
	logger     Logger
}

// NewCopier returns a new Copier to copy records
func NewCopier(srcTableService, trgTableService DynamoDBService, copierChan CopierChan, logger Logger) Copier {
	return copyService{
		srcTable:   srcTableService,
		trgTable:   trgTableService,
		copierChan: copierChan,
		logger:     logger,
	}
}

// Copy will copy all records from the source to target table.
// This method will create a worker pool according to the number of readers and writes that are passed as argument
func (service copyService) Copy(readers, writers int) error {
	service.logger.Printf("copying table with %d readers and %d writers", readers, writers)
	itemsChan, errChan := service.copierChan.Items, service.copierChan.Errors

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

// CopierChan encapsulates the value and error channel used by the copier
type CopierChan struct {
	Items  chan []DynamoDBItem
	Errors chan error
}

// NewCopierChan creates a new CopierChan with a buffered chan []DynamoDBItem of itemsChanSize
func NewCopierChan(itemsChanSize int) CopierChan {
	return CopierChan{
		Items:  make(chan []DynamoDBItem, itemsChanSize),
		Errors: make(chan error),
	}
}
