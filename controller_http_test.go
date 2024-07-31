package ctrl

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
	"github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/types"
	"github.com/metal-toolbox/rivets/condition"
	rtypes "github.com/metal-toolbox/rivets/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestRunTaskWithMonitor(t *testing.T) {
	// test for no leaked go routines
	defer goleak.VerifyNone(t, []goleak.Option{goleak.IgnoreCurrent()}...)

	ctx := context.Background()
	conditionID := uuid.New()
	serverID := uuid.New()
	conditionKind := condition.FirmwareInstall
	task := &condition.Task[any, any]{
		ID:     conditionID,
		Kind:   conditionKind,
		State:  condition.Pending,
		Server: &rtypes.Server{ID: serverID.String()},
	}

	testcases := []struct {
		name            string
		mocksetup       func() (*MockTaskHandler, *MockPublisher)
		publishInterval time.Duration
		handlerTimeout  time.Duration
		wantErr         bool
	}{
		{
			name: "handler executed successfully",
			mocksetup: func() (*MockTaskHandler, *MockPublisher) {
				publisher := NewMockPublisher(t)
				handler := NewMockTaskHandler(t)
				handler.On("HandleTask", mock.Anything, task, publisher).Times(1).
					//  sleep for 100ms
					Run(func(_ mock.Arguments) { time.Sleep(100 * time.Millisecond) }).
					Return(nil)

				//  ts update
				publisher.On("Publish", mock.Anything, task, true).Return(nil)
				return handler, publisher
			},
			publishInterval: 10 * time.Millisecond,
			handlerTimeout:  5 * time.Second,
		},
		{
			name: "handler returns error",
			mocksetup: func() (*MockTaskHandler, *MockPublisher) {
				publisher := NewMockPublisher(t)
				handler := NewMockTaskHandler(t)
				handler.On("HandleTask", mock.Anything, task, publisher).Times(1).
					Return(errors.New("barf"))

				publisher.On("Publish", mock.Anything, task, false).Return(nil)
				return handler, publisher
			},
			publishInterval: 10 * time.Second,
			handlerTimeout:  5 * time.Second,
			wantErr:         false, // The method doesn't return the handler error
		},
		{
			name: "handler panic",
			mocksetup: func() (*MockTaskHandler, *MockPublisher) {
				publisher := NewMockPublisher(t)
				handler := NewMockTaskHandler(t)
				handler.On("HandleTask", mock.Anything, task, publisher).Times(1).
					Run(func(_ mock.Arguments) { panic("oops") })

				publisher.On(
					"Publish",
					mock.Anything,
					mock.MatchedBy(func(task *condition.Task[any, any]) bool {
						assert.Equal(t, task.State, condition.Failed)
						return true
					}),
					false,
				).Return(nil)

				return handler, publisher
			},
			publishInterval: 30 * time.Second,
			handlerTimeout:  5 * time.Second,
			wantErr:         false, // The method recovers from panic
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			handler, publisher := tt.mocksetup()
			l := logrus.New()
			l.Level = logrus.ErrorLevel // set to DebugLevel for debugging

			controller := &HTTPController{
				logger:         l,
				serverID:       serverID,
				conditionKind:  conditionKind,
				handlerTimeout: tt.handlerTimeout,
			}

			err := controller.runTaskWithMonitor(ctx, handler, task, publisher, tt.publishInterval)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, len(publisher.Calls), 1, "expect at least one condition update")
				assert.Equal(t, 1, len(handler.Calls), "expect handler to be called once")
			}
		})
	}
}

func TestFetchTaskWithRetries(t *testing.T) {
	serverID := uuid.New()
	conditionKind := condition.FirmwareInstall
	tries := 3
	interval := time.Millisecond

	tests := []struct {
		name           string
		setupMock      func(*orc.MockQueryor)
		expectedResult *condition.Task[any, any]
		expectedError  error
	}{
		{
			name: "Successful fetch on first try",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						ID:    uuid.New(),
						Kind:  conditionKind,
						State: condition.Pending,
					},
				}, nil).Once()
			},
			expectedResult: &condition.Task[any, any]{
				Kind:  conditionKind,
				State: condition.Pending,
			},
			expectedError: nil,
		},
		{
			name: "Successful fetch after retry",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusInternalServerError,
					Condition: &condition.Condition{
						ID:    uuid.New(),
						Kind:  condition.Inventory,
						State: condition.Pending,
					},
				}, nil).Once()
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						ID:    uuid.New(),
						Kind:  conditionKind,
						State: condition.Pending,
					},
				}, nil).Once()
			},
			expectedResult: &condition.Task[any, any]{
				Kind:  conditionKind,
				State: condition.Pending,
			},
			expectedError: nil,
		},
		{
			name: "Fetch active condition and task",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						ID:    uuid.New(),
						Kind:  conditionKind,
						State: condition.Active,
					},
				}, nil).Once()
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Task: &condition.Task[any, any]{
						ID:    uuid.New(),
						Kind:  conditionKind,
						State: condition.Active,
					},
				}, nil).Once()
			},
			expectedResult: &condition.Task[any, any]{
				Kind:  conditionKind,
				State: condition.Active,
			},
			expectedError: nil,
		},
		{
			name: "Condition in final state",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						ID:    uuid.New(),
						Kind:  conditionKind,
						State: condition.Succeeded,
					},
				}, nil).Once()
			},
			expectedResult: nil,
			expectedError:  errNothingToDo,
		},
		{
			name: "Max retries reached",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						Kind: condition.Inventory,
					},
				}, nil).Times(tries + 1)
			},
			expectedResult: nil,
			expectedError:  errNothingToDo,
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ocm := orc.NewMockQueryor(t)
			tt.setupMock(ocm)

			controller := &HTTPController{
				logger:        logger,
				conditionKind: conditionKind,
				orcQueryor:    ocm,
			}

			result, err := controller.fetchTaskWithRetries(context.Background(), serverID, tries, interval)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedResult != nil {
				require.NotNil(t, result)
				assert.Equal(t, tt.expectedResult.Kind, result.Kind)
				assert.Equal(t, tt.expectedResult.State, result.State)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestFetchCondition(t *testing.T) {
	serverID := uuid.New()
	conditionKind := condition.FirmwareInstall

	tests := []struct {
		name           string
		setupMock      func(*orc.MockQueryor)
		expectedResult *condition.Condition
		expectedError  error
	}{
		{
			name: "Successful fetch",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Condition: &condition.Condition{
						ID:   uuid.New(),
						Kind: conditionKind,
					},
				}, nil)
			},
			expectedResult: &condition.Condition{
				Kind: conditionKind,
			},
			expectedError: nil,
		},
		{
			name: "Not Found error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusNotFound,
					Message:    "Condition not found",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
		{
			name: "Internal Server error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusInternalServerError,
					Message:    "Internal server error",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
		{
			name: "Bad Request error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusBadRequest,
					Message:    "Bad request",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errFetchCondition,
		},
		{
			name: "Unexpected status code",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusTeapot,
					Message:    "I'm a teapot",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errFetchCondition,
		},
		{
			name: "API call error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return((*types.ServerResponse)(nil), errors.New("API call failed"))
			},
			expectedResult: nil,
			expectedError:  errFetchCondition,
		},
		{
			name: "Empty response",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionQuery", mock.Anything, serverID).Return((*types.ServerResponse)(nil), nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ocm := orc.NewMockQueryor(t)
			tt.setupMock(ocm)

			controller := &HTTPController{
				logger:        logrus.New(),
				conditionKind: conditionKind,
				orcQueryor:    ocm,
			}

			result, err := controller.fetchCondition(context.Background(), serverID)

			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedResult != nil {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedResult.Kind, result.Kind)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestFetchTask(t *testing.T) {
	serverID := uuid.New()
	conditionKind := condition.FirmwareInstall

	tests := []struct {
		name           string
		setupMock      func(*orc.MockQueryor)
		expectedResult *condition.Task[any, any]
		expectedError  error
	}{
		{
			name: "Successful fetch",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusOK,
					Task: &condition.Task[any, any]{
						ID:   uuid.New(),
						Kind: conditionKind,
					},
				}, nil)
			},
			expectedResult: &condition.Task[any, any]{
				Kind: conditionKind,
			},
			expectedError: nil,
		},
		{
			name: "Not Found error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusNotFound,
					Message:    "Task not found",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
		{
			name: "Internal Server error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusInternalServerError,
					Message:    "Internal server error",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
		{
			name: "Bad Request error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusBadRequest,
					Message:    "Bad request",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errFetchTask,
		},
		{
			name: "Unexpected status code",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return(&types.ServerResponse{
					StatusCode: http.StatusTeapot,
					Message:    "barf",
				}, nil)
			},
			expectedResult: nil,
			expectedError:  errFetchTask,
		},
		{
			name: "API call error",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return((*types.ServerResponse)(nil), errors.New("API call failed"))
			},
			expectedResult: nil,
			expectedError:  errFetchTask,
		},
		{
			name: "Empty response",
			setupMock: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery", mock.Anything, conditionKind, serverID).Return((*types.ServerResponse)(nil), nil)
			},
			expectedResult: nil,
			expectedError:  errRetryRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ocm := orc.NewMockQueryor(t)
			tt.setupMock(ocm)

			controller := &HTTPController{
				logger:        logrus.New(),
				conditionKind: conditionKind,
				orcQueryor:    ocm,
			}

			result, err := controller.fetchTask(context.Background(), serverID)

			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedResult != nil {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedResult.Kind, result.Kind)
			} else {
				assert.Nil(t, result)
			}

		})
	}
}
