// Code generated by mockery v1.0.0
package mocks

import dynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
import dynamodbcopy "github.com/uniplaces/dynamodbcopy"
import mock "github.com/stretchr/testify/mock"

// DynamoDBService is an autogenerated mock type for the DynamoDBService type
type DynamoDBService struct {
	mock.Mock
}

// BatchWrite provides a mock function with given fields: items
func (_m *DynamoDBService) BatchWrite(items []dynamodbcopy.DynamoDBItem) error {
	ret := _m.Called(items)

	var r0 error
	if rf, ok := ret.Get(0).(func([]dynamodbcopy.DynamoDBItem) error); ok {
		r0 = rf(items)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DescribeTable provides a mock function with given fields:
func (_m *DynamoDBService) DescribeTable() (*dynamodb.TableDescription, error) {
	ret := _m.Called()

	var r0 *dynamodb.TableDescription
	if rf, ok := ret.Get(0).(func() *dynamodb.TableDescription); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*dynamodb.TableDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Scan provides a mock function with given fields: totalSegments, segment, itemsChan
func (_m *DynamoDBService) Scan(totalSegments int, segment int, itemsChan chan<- []dynamodbcopy.DynamoDBItem) error {
	ret := _m.Called(totalSegments, segment, itemsChan)

	var r0 error
	if rf, ok := ret.Get(0).(func(int, int, chan<- []dynamodbcopy.DynamoDBItem) error); ok {
		r0 = rf(totalSegments, segment, itemsChan)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateCapacity provides a mock function with given fields: capacity
func (_m *DynamoDBService) UpdateCapacity(capacity dynamodbcopy.Capacity) error {
	ret := _m.Called(capacity)

	var r0 error
	if rf, ok := ret.Get(0).(func(dynamodbcopy.Capacity) error); ok {
		r0 = rf(capacity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WaitForReadyTable provides a mock function with given fields:
func (_m *DynamoDBService) WaitForReadyTable() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
