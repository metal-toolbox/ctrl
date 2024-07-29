// Code generated by mockery v2.42.1. DO NOT EDIT.

package ctrl

import (
	context "context"

	condition "github.com/metal-toolbox/rivets/condition"

	mock "github.com/stretchr/testify/mock"
)

// MockTaskHandler is an autogenerated mock type for the TaskHandler type
type MockTaskHandler struct {
	mock.Mock
}

type MockTaskHandler_Expecter struct {
	mock *mock.Mock
}

func (_m *MockTaskHandler) EXPECT() *MockTaskHandler_Expecter {
	return &MockTaskHandler_Expecter{mock: &_m.Mock}
}

// HandleTask provides a mock function with given fields: ctx, task, statusPublisher
func (_m *MockTaskHandler) HandleTask(ctx context.Context, task *condition.Task[interface{}, interface{}], statusPublisher Publisher) error {
	ret := _m.Called(ctx, task, statusPublisher)

	if len(ret) == 0 {
		panic("no return value specified for HandleTask")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *condition.Task[interface{}, interface{}], Publisher) error); ok {
		r0 = rf(ctx, task, statusPublisher)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockTaskHandler_HandleTask_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HandleTask'
type MockTaskHandler_HandleTask_Call struct {
	*mock.Call
}

// HandleTask is a helper method to define mock.On call
//   - ctx context.Context
//   - task *condition.Task[interface{},interface{}]
//   - statusPublisher Publisher
func (_e *MockTaskHandler_Expecter) HandleTask(ctx interface{}, task interface{}, statusPublisher interface{}) *MockTaskHandler_HandleTask_Call {
	return &MockTaskHandler_HandleTask_Call{Call: _e.mock.On("HandleTask", ctx, task, statusPublisher)}
}

func (_c *MockTaskHandler_HandleTask_Call) Run(run func(ctx context.Context, task *condition.Task[interface{}, interface{}], statusPublisher Publisher)) *MockTaskHandler_HandleTask_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*condition.Task[interface{}, interface{}]), args[2].(Publisher))
	})
	return _c
}

func (_c *MockTaskHandler_HandleTask_Call) Return(_a0 error) *MockTaskHandler_HandleTask_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockTaskHandler_HandleTask_Call) RunAndReturn(run func(context.Context, *condition.Task[interface{}, interface{}], Publisher) error) *MockTaskHandler_HandleTask_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockTaskHandler creates a new instance of MockTaskHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockTaskHandler(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockTaskHandler {
	mock := &MockTaskHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
