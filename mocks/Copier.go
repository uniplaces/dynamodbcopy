// Code generated by mockery v1.0.0
package mocks

import mock "github.com/stretchr/testify/mock"

// Copier is an autogenerated mock type for the Copier type
type Copier struct {
	mock.Mock
}

// Copy provides a mock function with given fields: readers, writers
func (_m *Copier) Copy(readers int, writers int) error {
	ret := _m.Called(readers, writers)

	var r0 error
	if rf, ok := ret.Get(0).(func(int, int) error); ok {
		r0 = rf(readers, writers)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
