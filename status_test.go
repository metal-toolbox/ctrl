package ctrl

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	srvtest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
	orctypes "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/types"
	"github.com/metal-toolbox/rivets/condition"
	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/registry"
)

func startJetStreamServer(t *testing.T) *server.Server {
	t.Helper()
	opts := srvtest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return srvtest.RunServer(&opts)
}

func jetStreamContext(t *testing.T, s *server.Server) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect => %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("JetStream => %v", err)
	}
	return nc, js
}

func shutdownJetStream(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func TestNewNatsConditionStatusPublisher(t *testing.T) {
	srv := startJetStreamServer(t)
	defer shutdownJetStream(t, srv)
	natsConn, _ := jetStreamContext(t, srv) // nc is closed on evJS.Close(), js needs no cleanup
	evJS := events.NewJetstreamFromConn(natsConn)
	defer evJS.Close()

	cond := &condition.Condition{
		Kind: condition.FirmwareInstall,
		ID:   uuid.New(),
	}

	const facilityCode = "fac13"
	controllerID := registry.GetID("test")

	controller := &NatsController{
		stream:        evJS,
		facilityCode:  facilityCode,
		conditionKind: cond.Kind,
		logger:        logrus.New(),
	}

	// test happy case
	p, err := NewNatsConditionStatusPublisher(
		"test",
		cond.ID.String(),
		facilityCode,
		cond.Kind,
		controllerID,
		0,
		evJS,
		controller.logger,
	)

	publisher, ok := p.(*NatsConditionStatusPublisher)
	assert.True(t, ok)

	require.Nil(t, err)
	require.NotNil(t, publisher, "publisher constructor")

	assert.Equal(t, controller.facilityCode, publisher.facilityCode)
	assert.Equal(t, cond.ID.String(), publisher.conditionID)
	assert.Equal(t, controller.logger, publisher.log)

	// Test re-initialized publisher will set lastRev to KV revision and subsequent publishes work
	serverID := uuid.New()
	require.NotPanics(t,
		func() {
			errP := publisher.Publish(
				context.Background(),
				serverID.String(),
				condition.Pending,
				[]byte(`{"pending...": "true"}`),
				false,
			)
			require.NoError(t, errP)
		},
		"publish 1",
	)
	require.Equal(t, uint64(1), publisher.lastRev)

	p, err = NewNatsConditionStatusPublisher(
		"test",
		cond.ID.String(),
		facilityCode,
		cond.Kind,
		controllerID,
		0,
		evJS,
		controller.logger,
	)

	publisher, ok = p.(*NatsConditionStatusPublisher)
	assert.True(t, ok)

	require.Nil(t, err)
	require.NotNil(t, publisher, "publisher constructor")
	require.Equal(t, uint64(1), publisher.lastRev)

	require.NotPanics(t,
		func() {
			errP := publisher.Publish(
				context.Background(),
				serverID.String(),
				condition.Active,
				[]byte(`{"some work...": "true"}`),
				false,
			)
			require.NoError(t, errP)
		},
		"publish 2",
	)
	require.Equal(t, uint64(2), publisher.lastRev)
}

func TestPublish(t *testing.T) {
	srv := startJetStreamServer(t)
	defer shutdownJetStream(t, srv)
	natsConn, jsCtx := jetStreamContext(t, srv) // nc is closed on evJS.Close(), js needs no cleanup
	evJS := events.NewJetstreamFromConn(natsConn)
	defer evJS.Close()

	cond := &condition.Condition{
		Kind: condition.FirmwareInstall,
		ID:   uuid.New(),
	}

	const facilityCode = "fac13"

	controllerID := registry.GetID("test")

	controller := &NatsController{
		stream:        evJS,
		facilityCode:  facilityCode,
		conditionKind: cond.Kind,
		logger:        logrus.New(),
	}

	p, err := NewNatsConditionStatusPublisher(
		"test",
		cond.ID.String(),
		facilityCode,
		cond.Kind,
		controllerID,
		0,
		evJS,
		controller.logger,
	)

	publisher, ok := p.(*NatsConditionStatusPublisher)
	assert.True(t, ok)

	require.Nil(t, err)
	require.NotNil(t, publisher, "publisher constructor")

	kv, err := jsCtx.KeyValue(string(cond.Kind))
	require.NoError(t, err, "kv read handle")

	serverID := uuid.New()

	// publish pending status
	require.NotPanics(t,
		func() {
			errP := publisher.Publish(
				context.Background(), serverID.String(),
				condition.Pending,
				[]byte(`{"pending...": "true"}`),
				false,
			)
			require.NoError(t, errP)
		},
		"publish pending",
	)
	require.NotEqual(t, 0, publisher.lastRev, "last rev - 1")

	entry, err := kv.Get(facilityCode + "." + cond.ID.String())
	require.Nil(t, err)
	require.Equal(t, entry.Revision(), publisher.lastRev, "last rev - 2")

	sv := &condition.StatusValue{}
	err = json.Unmarshal(entry.Value(), sv)
	require.NoError(t, err, "unmarshal")

	require.Equal(t, condition.StatusValueVersion, sv.MsgVersion, "version check")
	require.Equal(t, serverID.String(), sv.Target, "sv Target")
	require.Contains(t, string(sv.Status), condition.Pending, "sv Status")

	// publish active status
	require.NotPanics(t,
		func() {
			errP := publisher.Publish(
				context.Background(),
				serverID.String(),
				condition.Active,
				[]byte(`{"active...": "true"}`),
				false,
			)
			require.NoError(t, errP)

		},
		"publish active",
	)

	entry, err = kv.Get(facilityCode + "." + cond.ID.String())
	require.Nil(t, err)
	require.Equal(t, entry.Revision(), publisher.lastRev, "last rev - 3")
}

func TestConditionState(t *testing.T) {
	srv := startJetStreamServer(t)
	defer shutdownJetStream(t, srv)
	natsConn, jsCtx := jetStreamContext(t, srv) // nc is closed on evJS.Close(), js needs no cleanup
	evJS := events.NewJetstreamFromConn(natsConn)
	defer evJS.Close()

	kvName := "testKV"
	_, err := jsCtx.CreateKeyValue(&nats.KeyValueConfig{Bucket: kvName})
	require.NoError(t, err)

	kvStore, err := jsCtx.KeyValue(kvName)
	require.NoError(t, err)

	tests := []struct {
		name        string
		setup       func(string, nats.KeyValue)
		conditionID string
		expected    *condition.StatusValue
	}{
		{
			name: "Condition not started",
			// nolint:revive // function param names I'd like to keep around, k thx revive
			setup: func(conditionID string, kv nats.KeyValue) {
				// No setup needed as no KV entry will exist
			},
		},
		{
			name: "Condition complete",
			setup: func(conditionID string, kv nats.KeyValue) {
				statusValue := &condition.StatusValue{
					State: string(condition.Succeeded),
					// Populate other required fields
				}
				data, _ := json.Marshal(statusValue)
				if _, err := kv.Put("testFacility."+conditionID, data); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Condition indeterminate (unreadable status)",
			setup: func(conditionID string, kv nats.KeyValue) {
				// Put unreadable data
				if _, err := kv.Put("testFacility."+conditionID, []byte("not json")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Condition orphaned (missing worker data)",
			setup: func(conditionID string, kv nats.KeyValue) {
				statusValue := &condition.StatusValue{
					State:    string(condition.Pending),
					WorkerID: "missingWorker",
					// Populate other required fields
				}
				data, _ := json.Marshal(statusValue)
				if _, err := kv.Put("testFacility."+conditionID, data); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt.conditionID = uuid.New().String()
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.conditionID, kvStore)

			entry, err := kvStore.Get("testFacility." + tt.conditionID)
			if errors.Is(err, nats.ErrKeyNotFound) && tt.name == "Condition not started" {
				// Expected path for not started condition
				return
			}

			require.NoError(t, err, "Expect no error fetching entry")
			var sv condition.StatusValue

			err = json.Unmarshal(entry.Value(), &sv)
			if tt.name == "Condition indeterminate (unreadable status)" {
				require.Error(t, err, "Expect error on unmarshal for indeterminate condition")
			} else {
				require.NoError(t, err, "Expect successful unmarshal for condition status")
			}
		})
	}
}

func TestStatusValueUpdate(t *testing.T) {
	tests := []struct {
		name                string
		curSV               *condition.StatusValue
		newSV               *condition.StatusValue
		expectedSV          *condition.StatusValue
		expectedErrContains string
	}{
		{
			name: "Successful status update with different states",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Pending),
				Status:   json.RawMessage(`{"msg":"status1"}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{"msg":"status2"}`),
			},
			expectedSV: &condition.StatusValue{
				WorkerID:  "worker1",
				Target:    "target1",
				TraceID:   "trace1",
				SpanID:    "span1",
				State:     string(condition.Active),
				Status:    json.RawMessage(`{"msg":"status2"}`),
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "Error returned when update on a finalized condition",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Succeeded),
				Status:   json.RawMessage(`{"msg":"status1"}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{"msg":"status2"}`),
			},
			expectedSV:          nil,
			expectedErrContains: "invalid update, condition state already finalized",
		},
		{
			name: "Error returned for invalid current Status JSON",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Active),
				Status:   json.RawMessage(`{"msg":"status1}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{"msg":"status2"}`),
			},
			expectedSV:          nil,
			expectedErrContains: "current StatusValue unmarshal error",
		},
		{
			name: "Error returned for invalid new Status JSON",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Active),
				Status:   json.RawMessage(`{"msg":"status1"}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{"msg":"status2}`),
			},
			expectedSV:          nil,
			expectedErrContains: "new StatusValue unmarshal error",
		},
		{
			name: "Empty status update does not overwrite current",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Active),
				Status:   json.RawMessage(`{"msg":"hello"}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{}`),
			},
			expectedSV: &condition.StatusValue{
				WorkerID:  "worker1",
				Target:    "target1",
				TraceID:   "trace1",
				SpanID:    "span1",
				State:     string(condition.Active),
				Status:    json.RawMessage(`{"msg":"hello"}`),
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "Empty new State does not overwrite current",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Active),
				Status:   json.RawMessage(`{"msg":"hello"}`),
			},
			newSV: &condition.StatusValue{
				Status: json.RawMessage(`{"msg": "hello 2"}`),
			},
			expectedSV: &condition.StatusValue{
				WorkerID:  "worker1",
				Target:    "target1",
				TraceID:   "trace1",
				SpanID:    "span1",
				State:     string(condition.Active),
				Status:    json.RawMessage(`{"msg":"hello 2"}`),
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "No update when status is equal",
			curSV: &condition.StatusValue{
				WorkerID: "worker1",
				Target:   "target1",
				TraceID:  "trace1",
				SpanID:   "span1",
				State:    string(condition.Active),
				Status:   json.RawMessage(`{"msg":"same"}`),
			},
			newSV: &condition.StatusValue{
				State:  string(condition.Active),
				Status: json.RawMessage(`{"msg":"same"}`),
			},
			expectedSV: &condition.StatusValue{
				WorkerID:  "worker1",
				Target:    "target1",
				TraceID:   "trace1",
				SpanID:    "span1",
				State:     string(condition.Active),
				Status:    json.RawMessage(`{"msg":"same"}`),
				UpdatedAt: time.Now(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSV, err := statusValueUpdate(tt.curSV, tt.newSV)

			if tt.expectedErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErrContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSV.WorkerID, gotSV.WorkerID)
				assert.Equal(t, tt.expectedSV.Target, gotSV.Target)
				assert.Equal(t, tt.expectedSV.TraceID, gotSV.TraceID)
				assert.Equal(t, tt.expectedSV.SpanID, gotSV.SpanID)
				assert.Equal(t, tt.expectedSV.State, gotSV.State)
				assert.JSONEq(t, string(tt.expectedSV.Status), string(gotSV.Status))
				assert.WithinDuration(t, tt.expectedSV.UpdatedAt, gotSV.UpdatedAt, time.Second)
			}
		})
	}
}

func TestHTTPConditionStatusPublisher_Publish(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	serverID := uuid.New()
	conditionID := uuid.New()
	conditionKind := condition.FirmwareInstall
	controllerID := registry.GetID("test-controller")

	publisher := &HTTPConditionStatusPublisher{
		conditionID:   conditionID,
		conditionKind: conditionKind,
		serverID:      serverID,
		controllerID:  controllerID,
		logger:        logger,
	}

	tests := []struct {
		name          string
		state         condition.State
		status        json.RawMessage
		tsUpdateOnly  bool
		mockSetup     func(m *orc.MockQueryor)
		expectedError string
	}{
		{
			name:         "Successful publish",
			state:        condition.Active,
			status:       json.RawMessage(`{"message":"In progress"}`),
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionStatusUpdate",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					controllerID,
					mock.MatchedBy(func(sv *condition.StatusValue) bool {
						assert.Equal(t, sv.State, string(condition.Active))
						assert.Equal(t, sv.Target, serverID.String())
						assert.Equal(t, sv.WorkerID, controllerID.String())
						assert.WithinDuration(t, time.Now(), sv.UpdatedAt, time.Second)

						return true
					}),
					false,
				).Return(&orctypes.ServerResponse{StatusCode: 200}, nil)
			},
		},
		{
			name:         "Successful timestamp-only update",
			state:        condition.Active,
			status:       json.RawMessage(`{"message":"In progress"}`),
			tsUpdateOnly: true,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionStatusUpdate",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					controllerID,
					mock.IsType(&condition.StatusValue{}),
					true,
				).Return(&orctypes.ServerResponse{StatusCode: 200}, nil)
			},
		},
		{
			name:         "Publish error",
			state:        condition.Failed,
			status:       json.RawMessage(`{"error":"Something went wrong"}`),
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionStatusUpdate",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					controllerID,
					mock.IsType(&condition.StatusValue{}),
					false,
				).Return(nil, errors.New("Publish error"))
			},
			expectedError: "Publish error",
		},
		{
			name:         "Non-200 status code",
			state:        condition.Succeeded,
			status:       json.RawMessage(`{"message":"Completed successfully"}`),
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionStatusUpdate",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					controllerID,
					mock.IsType(&condition.StatusValue{}),
					false,
				).Return(&orctypes.ServerResponse{StatusCode: 400}, nil)
			},
			expectedError: "API Query returned error, status code: 400: condition status publish error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryor := new(orc.MockQueryor)
			tt.mockSetup(mockQueryor)
			publisher.orcQueryor = mockQueryor

			ctx := context.Background()
			err := publisher.Publish(ctx, serverID.String(), tt.state, tt.status, tt.tsUpdateOnly)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
