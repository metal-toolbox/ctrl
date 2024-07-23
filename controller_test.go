package controller

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"

	"github.com/metal-toolbox/rivets/condition"
	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/registry"
)

func TestNewNatsControllerWithOptions(t *testing.T) {
	tests := []struct {
		name     string
		option   Option
		validate func(*testing.T, *NatsController)
	}{
		{
			name:   "WithLogger",
			option: WithLogger(logrus.New()),
			validate: func(t *testing.T, nc *NatsController) {
				assert.NotNil(t, nc.logger, "logger should not be nil")
			},
		},
		{
			name:   "WithConcurrency",
			option: WithConcurrency(10),
			validate: func(t *testing.T, nc *NatsController) {
				assert.Equal(t, 10, nc.concurrency, "concurrency should match the expected value")
			},
		},
		{
			name:   "WithKVReplicas",
			option: WithKVReplicas(0),
			validate: func(t *testing.T, nc *NatsController) {
				assert.Equal(t, 0, nc.natsConfig.KVReplicationFactor, "kvReplicas should match the expected value")
			},
		},
		{
			name:   "WithHandlerTimeout",
			option: WithHandlerTimeout(15 * time.Minute),
			validate: func(t *testing.T, nc *NatsController) {
				assert.Equal(t, 15*time.Minute, nc.handlerTimeout, "handlerTimeout should match the expected value")
			},
		},
		{
			name:   "WithConnectionTimeout",
			option: WithConnectionTimeout(45 * time.Second),
			validate: func(t *testing.T, nc *NatsController) {
				assert.Equal(t, 45*time.Second, nc.natsConfig.ConnectTimeout, "connectionTimeout should match the expected value")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := &NatsController{}
			// set option
			tc.option(nc)
			// verify
			tc.validate(t, nc)
		})
	}
}

func TestNatsControllerDefaultParameters(t *testing.T) {
	c := NewNatsController(
		"app",
		"facility",
		"firmwareInstall",
		"nats://localhost:4222",
		"/etc/nats/creds.file",
		condition.FirmwareInstall,
	)

	expectedHostname, _ := os.Hostname()

	assert.Equal(t, expectedHostname, c.hostname, "hostname should match the expected default value")
	assert.Equal(t, "facility", c.facilityCode, "facilityCode should match the expected default value")
	assert.Equal(t, condition.FirmwareInstall, c.conditionKind, "conditionKind should match the expected default value")
	assert.Equal(t, connectionTimeout, c.connectionTimeout, "connectionTimeout should match the expected default value")
	assert.Equal(t, handlerTimeout, c.handlerTimeout, "handlerTimeout should match the expected default value")
	assert.Equal(t, pullEventInterval, c.pullEventInterval, "pullEventInterval should match the expected default value")
	assert.Equal(t, pullEventTimeout, c.pullEventTimeout, "pullEventTimeout should match the expected default value")
	assert.Equal(t, concurrency, c.concurrency, "concurrency should match the expected default value")
	assert.Equal(t, kvReplicationFactor, c.natsConfig.KVReplicationFactor, "kv replicas should match the expected default value")
}

func TestStateFinalized(t *testing.T) {
	tests := []struct {
		name         string
		conditionID  uuid.UUID
		state        ConditionState
		expectMethod string
		expectBool   bool
	}{
		{
			name:         "Condition in progress",
			conditionID:  uuid.New(),
			state:        inProgress,
			expectMethod: "inProgress",
			expectBool:   false,
		},
		{
			name:         "Condition complete",
			conditionID:  uuid.New(),
			state:        complete,
			expectMethod: "complete",
			expectBool:   false,
		},
		{
			name:         "Condition orphaned",
			conditionID:  uuid.New(),
			state:        orphaned,
			expectMethod: "inProgress",
			expectBool:   true,
		},
		{
			name:         "Condition not started",
			conditionID:  uuid.New(),
			state:        notStarted,
			expectMethod: "inProgress",
			expectBool:   true,
		},
		{
			name:         "Condition state indeterminate",
			conditionID:  uuid.New(),
			state:        indeterminate,
			expectMethod: "nak",
			expectBool:   false,
		},
		{
			name:         "Condition state unexpected",
			conditionID:  uuid.New(),
			state:        99, // unexpected lifecycle state
			expectMethod: "complete",
			expectBool:   false,
		},
	}

	controllerID := registry.GetID("test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := &condition.Condition{ID: tt.conditionID}

			eStatusAcknowledger := NewMockeventStatusAcknowleger(t)
			eStatusAcknowledger.On(tt.expectMethod).Return()

			cStatusQueryor := NewMockConditionStatusQueryor(t)
			cStatusQueryor.On("ConditionState", tt.conditionID.String()).Return(tt.state)

			mockLiveness := NewMockLivenessCheckin(t)
			mockLiveness.On("ControllerID").Return(controllerID)

			l := logrus.New()
			l.SetOutput(io.Discard) // unset this to debug
			n := &NatsController{logger: l, liveness: mockLiveness}

			gotBool := n.stateFinalized(context.Background(), cond, cStatusQueryor, eStatusAcknowledger)
			assert.Equal(t, tt.expectBool, gotBool)
		})
	}
}

func TestProcessCondition(t *testing.T) {
	cond := &condition.Condition{ID: uuid.New()}
	tests := []struct {
		name      string
		setupMock func(t *testing.T) (*MockPublisher, *MockeventStatusAcknowleger, *MockLivenessCheckin)
	}{
		{
			name: "First KV status publish failure",
			setupMock: func(t *testing.T) (*MockPublisher, *MockeventStatusAcknowleger, *MockLivenessCheckin) {
				p := NewMockPublisher(t)
				p.On(
					"Publish",
					mock.Anything,
					mock.MatchedBy(func(task *condition.Task[any, any]) bool {
						assert.Equal(t, cond.ID, task.ID)
						return true
					}),
					false,
				).Return(errors.New("bytes exploded"))

				sa := NewMockeventStatusAcknowleger(t)
				sa.On("nak").Return()

				controllerID := registry.GetID("test")
				lv := NewMockLivenessCheckin(t)
				lv.On("ControllerID").Return(controllerID)

				return p, sa, lv
			},
		},
		{
			name: "First KV status publish success",
			setupMock: func(t *testing.T) (*MockPublisher, *MockeventStatusAcknowleger, *MockLivenessCheckin) {
				p := NewMockPublisher(t)
				p.On(
					"Publish",
					mock.Anything,
					mock.MatchedBy(func(task *condition.Task[any, any]) bool {
						assert.Equal(t, cond.ID, task.ID)
						return true
					}),
					false,
				).Return(nil)

				sa := NewMockeventStatusAcknowleger(t)
				sa.On("complete").Return()

				controllerID := registry.GetID("test")
				lv := NewMockLivenessCheckin(t)
				lv.On("ControllerID").Return(controllerID)

				return p, sa, lv
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logrus.New()
			l.SetOutput(io.Discard) // unset this to debug

			publisher, eStatusAcknowledger, liveness := tt.setupMock(t)
			n := &NatsController{
				logger:   l,
				liveness: liveness,
			}

			n.processCondition(context.TODO(), cond, publisher, eStatusAcknowledger)
		})
	}
}

func TestConditionFromEvent(t *testing.T) {
	conditionID := uuid.New()

	tests := []struct {
		name    string
		data    func() []byte
		want    *condition.Condition
		wantErr bool
	}{
		{
			name:    "data field empty",
			data:    func() []byte { return nil },
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unable to deserialize condition",
			data:    func() []byte { return []byte(`invalid json`) },
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid condition",
			data: func() []byte {
				condition := &condition.Condition{
					Version:    "1",
					ID:         conditionID,
					Kind:       "test-kind",
					Parameters: json.RawMessage(`{"foo":"bar"}`),
				}
				data, _ := json.Marshal(condition)
				return data
			},
			wantErr: false,
			want: func() *condition.Condition {
				condition := &condition.Condition{
					Version:    "1",
					ID:         uuid.New(),
					Kind:       "test-kind",
					Parameters: json.RawMessage(`{"foo":"bar"}`),
				}
				return condition
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMsg := events.NewMockMessage(t)
			mockMsg.On("Data").Return(tt.data())

			got, err := conditionFromEvent(mockMsg)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want.Version, got.Version)
				assert.Equal(t, conditionID.String(), got.ID.String())
				assert.Equal(t, tt.want.Kind, got.Kind)
				assert.Equal(t, tt.want.Parameters, got.Parameters)
			}

			mockMsg.AssertExpectations(t)
		})
	}
}

func TestRunTaskHandlerWithMonitor(t *testing.T) {
	// test for no leaked go routines
	// Ignore can be removed if this ever gets fixed,
	//
	// https://github.com/census-instrumentation/opencensus-go/issues/1191
	defer goleak.VerifyNone(t, []goleak.Option{goleak.IgnoreCurrent()}...)

	// test monitor calls ackInprogress
	ctx := context.Background()
	cond := &condition.Condition{Kind: condition.FirmwareInstall}

	testcases := []struct {
		name            string
		mocksetup       func() (*MockTaskHandler, *events.MockMessage, *MockPublisher)
		publishInterval time.Duration
		wantErr         bool
	}{
		{
			name: "handler executed successfully",
			mocksetup: func() (*MockTaskHandler, *events.MockMessage, *MockPublisher) {
				publisher := NewMockPublisher(t)
				message := events.NewMockMessage(t)
				handler := NewMockTaskHandler(t)

				handler.On("HandleTask", mock.Anything, mock.IsType(&condition.Task[any, any]{}), publisher).Times(1).
					//  sleep for 100ms
					Run(func(_ mock.Arguments) { time.Sleep(100 * time.Millisecond) }).
					Return(nil)

				// ts update
				publisher.On("Publish", mock.Anything, mock.IsType(&condition.Task[any, any]{}), true).Return(nil)
				return handler, message, publisher
			},
			publishInterval: 10 * time.Millisecond,
		},
		{
			name: "handler returns error",
			mocksetup: func() (*MockTaskHandler, *events.MockMessage, *MockPublisher) {
				publisher := NewMockPublisher(t)
				message := events.NewMockMessage(t)
				handler := NewMockTaskHandler(t)

				handler.On("HandleTask", mock.Anything, mock.IsType(&condition.Task[any, any]{}), publisher).Times(1).
					Return(errors.New("barf"))

				return handler, message, publisher
			},
			publishInterval: 10 * time.Second, // high interval to allow this case to focus on a handler failure
			wantErr:         true,
		},
		{
			name: "status publish fails",
			mocksetup: func() (*MockTaskHandler, *events.MockMessage, *MockPublisher) {
				publisher := NewMockPublisher(t)
				message := events.NewMockMessage(t)
				handler := NewMockTaskHandler(t)

				handler.On("HandleTask", mock.Anything, mock.IsType(&condition.Task[any, any]{}), publisher).Times(1).
					//  sleep for 100ms
					Run(func(_ mock.Arguments) { time.Sleep(100 * time.Millisecond) }).
					Return(nil)

				publisher.On("Publish", mock.Anything, mock.IsType(&condition.Task[any, any]{}), true).Return(errors.New("barf"))

				return handler, message, publisher
			},
			publishInterval: 10 * time.Millisecond,
			wantErr:         false,
		},
		{
			name: "handler panic",
			mocksetup: func() (*MockTaskHandler, *events.MockMessage, *MockPublisher) {
				publisher := NewMockPublisher(t)
				message := events.NewMockMessage(t)
				handler := NewMockTaskHandler(t)

				handler.On("HandleTask", mock.Anything, mock.IsType(&condition.Task[any, any]{}), publisher).Times(1).
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
				return handler, message, publisher
			},
			publishInterval: 30 * time.Second,
			wantErr:         true,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, publisher := tt.mocksetup()
			l := logrus.New()
			l.Level = 0 // set higher value to debug
			n := &NatsController{
				logger: l,
				conditionHandlerFactory: func() TaskHandler {
					return handler
				},
			}

			err := n.runTaskHandlerWithMonitor(
				ctx,
				condition.NewTaskFromCondition(cond),
				publisher,
				tt.publishInterval,
			)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, len(publisher.Calls), 9, "expect multiple condition TS updates")
				assert.Equal(t, 1, len(handler.Calls), "expect handler to be called once")
			}
		})
	}
}
