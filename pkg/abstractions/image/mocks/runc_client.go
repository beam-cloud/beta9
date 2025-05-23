// Code generated by mockery v2.43.1. DO NOT EDIT.

package mocks

import (
	context "context"

	common "github.com/beam-cloud/beta9/pkg/common"

	mock "github.com/stretchr/testify/mock"

	proto "github.com/beam-cloud/beta9/proto"
)

// RuncClient is an autogenerated mock type for the RuncClient type
type RuncClient struct {
	mock.Mock
}

// Archive provides a mock function with given fields: ctx, containerId, imageId, outputChan
func (_m *RuncClient) Archive(ctx context.Context, containerId string, imageId string, outputChan chan common.OutputMsg) error {
	ret := _m.Called(ctx, containerId, imageId, outputChan)

	if len(ret) == 0 {
		panic("no return value specified for Archive")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, chan common.OutputMsg) error); ok {
		r0 = rf(ctx, containerId, imageId, outputChan)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Exec provides a mock function with given fields: containerId, command, env
func (_m *RuncClient) Exec(containerId string, command string, env []string) (*proto.RunCExecResponse, error) {
	ret := _m.Called(containerId, command, env)

	if len(ret) == 0 {
		panic("no return value specified for Exec")
	}

	var r0 *proto.RunCExecResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string, []string) (*proto.RunCExecResponse, error)); ok {
		return rf(containerId, command, env)
	}
	if rf, ok := ret.Get(0).(func(string, string, []string) *proto.RunCExecResponse); ok {
		r0 = rf(containerId, command, env)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*proto.RunCExecResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(string, string, []string) error); ok {
		r1 = rf(containerId, command, env)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Kill provides a mock function with given fields: containerId
func (_m *RuncClient) Kill(containerId string) (*proto.RunCKillResponse, error) {
	ret := _m.Called(containerId)

	if len(ret) == 0 {
		panic("no return value specified for Kill")
	}

	var r0 *proto.RunCKillResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*proto.RunCKillResponse, error)); ok {
		return rf(containerId)
	}
	if rf, ok := ret.Get(0).(func(string) *proto.RunCKillResponse); ok {
		r0 = rf(containerId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*proto.RunCKillResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(containerId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Status provides a mock function with given fields: containerId
func (_m *RuncClient) Status(containerId string) (*proto.RunCStatusResponse, error) {
	ret := _m.Called(containerId)

	if len(ret) == 0 {
		panic("no return value specified for Status")
	}

	var r0 *proto.RunCStatusResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*proto.RunCStatusResponse, error)); ok {
		return rf(containerId)
	}
	if rf, ok := ret.Get(0).(func(string) *proto.RunCStatusResponse); ok {
		r0 = rf(containerId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*proto.RunCStatusResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(containerId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StreamLogs provides a mock function with given fields: ctx, containerId, outputChan
func (_m *RuncClient) StreamLogs(ctx context.Context, containerId string, outputChan chan common.OutputMsg) error {
	ret := _m.Called(ctx, containerId, outputChan)

	if len(ret) == 0 {
		panic("no return value specified for StreamLogs")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, chan common.OutputMsg) error); ok {
		r0 = rf(ctx, containerId, outputChan)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewRuncClient creates a new instance of RuncClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRuncClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *RuncClient {
	mock := &RuncClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
