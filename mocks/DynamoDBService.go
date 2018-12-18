// Code generated by mockery v1.0.0
package mocks

import dynamodb "github.com/aws/aws-sdk-go/service/dynamodb"

import mock "github.com/stretchr/testify/mock"

// DynamoDBService is an autogenerated mock type for the DynamoDBService type
type DynamoDBService struct {
	mock.Mock
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

// UpdateCapacity provides a mock function with given fields: read, write
func (_m *DynamoDBService) UpdateCapacity(read int64, write int64) error {
	ret := _m.Called(read, write)

	var r0 error
	if rf, ok := ret.Get(0).(func(int64, int64) error); ok {
		r0 = rf(read, write)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}