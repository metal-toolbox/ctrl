package ctrl

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
	orctypes "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/types"
	"github.com/metal-toolbox/rivets/v2/condition"
	"github.com/metal-toolbox/rivets/v2/events"
	"github.com/metal-toolbox/rivets/v2/events/registry"
	"github.com/nats-io/nats-server/v2/server"
	srvtest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func runNATSServer(t *testing.T) *server.Server {
	opts := srvtest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	return srvtest.RunServer(&opts)
}

func shutdownNATSServer(t *testing.T, s *server.Server) {
	t.Helper()
	s.Shutdown()
	s.WaitForShutdown()
}

func TestNewNatsConditionTaskRepository(t *testing.T) {
	// Start a NATS server
	ns := runNATSServer(t)
	defer shutdownNATSServer(t, ns)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	// nats JS from conn - events type, for passing into NewNatsConditionTaskRepository
	njs := events.NewJetstreamFromConn(nc)
	defer njs.Close()

	// create KV bucket
	js, err := nc.JetStream()
	require.NoError(t, err)

	taskKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: condition.TaskKVRepositoryBucket,
	})
	require.NoError(t, err)

	facilityCode := "area13"
	serverID := uuid.New().String()
	conditionKind := condition.FirmwareInstall
	controllerID := registry.GetID("test-controller")
	logger := logrus.New()

	tests := []struct {
		name          string
		conditionID   string
		setupKV       func(kv nats.KeyValue)
		expectedError string
		validate      func(*testing.T, ConditionTaskRepository)
	}{
		{
			name:        "successful constructor init",
			conditionID: uuid.New().String(),
			setupKV:     func(_ nats.KeyValue) {},
			validate: func(t *testing.T, repo ConditionTaskRepository) {
				assert.NotNil(t, repo)
				taskRepo, ok := repo.(*NatsConditionTaskRepository)
				require.True(t, ok)
				assert.Equal(t, facilityCode, taskRepo.facilityCode)
				assert.Equal(t, serverID, taskRepo.serverID)
				assert.Equal(t, conditionKind, taskRepo.conditionKind)
				assert.Equal(t, controllerID.String(), taskRepo.controllerID)
				assert.NotNil(t, taskRepo.log)
				assert.Zero(t, taskRepo.lastRev)
			},
		},
		{
			name:        "existing completed task purged",
			conditionID: uuid.New().String(),
			setupKV: func(kv nats.KeyValue) {
				key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
				task := &condition.Task[any, any]{
					ID:    uuid.New(),
					State: condition.Succeeded,
				}
				taskBytes, _ := task.Marshal()
				_, err := kv.Put(key, taskBytes)
				require.NoError(t, err)
			},
			validate: func(t *testing.T, repo ConditionTaskRepository) {
				assert.NotNil(t, repo)
				// Verify that the completed task was purged
				taskRepo, ok := repo.(*NatsConditionTaskRepository)
				require.True(t, ok)
				key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
				_, err := taskRepo.kv.Get(key)
				assert.ErrorIs(t, err, nats.ErrKeyNotFound)
				// purged task has no revision set
				assert.Zero(t, taskRepo.lastRev)
			},
		},
		{
			name:        "existing task in incomplete state with different ID",
			conditionID: uuid.New().String(),
			setupKV: func(kv nats.KeyValue) {
				key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
				task := &condition.Task[any, any]{
					ID:    uuid.New(),
					State: condition.Active,
				}
				taskBytes, _ := task.Marshal()
				_, err := kv.Put(key, taskBytes)
				require.NoError(t, err)
			},
			expectedError: "existing Task object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupKV(taskKV)

			repo, err := NewNatsConditionTaskRepository(
				tt.conditionID,
				serverID,
				facilityCode,
				conditionKind,
				controllerID,
				0,
				njs,
				logger,
			)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				tt.validate(t, repo)
			}
		})
	}
}

func cleanKV(t *testing.T, kv nats.KeyValue) {
	t.Helper()

	// clean up all keys for this test
	purge, err := kv.Keys()
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		t.Fatal(err)
	}

	for _, k := range purge {
		if err := kv.Delete(k); err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatal(err)
		}
	}
}

func TestNatsConditionTaskRepository_Query(t *testing.T) {
	// Start a NATS server
	ns := runNATSServer(t)
	defer shutdownNATSServer(t, ns)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	// nats JS from conn - events type, for passing into NewNatsConditionTaskRepository
	njs := events.NewJetstreamFromConn(nc)
	defer njs.Close()

	// create KV bucket
	js, err := nc.JetStream()
	require.NoError(t, err)

	taskKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: condition.TaskKVRepositoryBucket,
	})
	require.NoError(t, err)

	facilityCode := "area13"
	serverID := uuid.New().String()
	conditionID := uuid.New().String()
	conditionKind := condition.FirmwareInstall
	controllerID := registry.GetID("test-controller")
	logger := logrus.New()

	tests := []struct {
		name               string
		setupKV            func(kv nats.KeyValue)
		expectedTask       *condition.Task[any, any]
		expectedQueryError string
		expectedInitError  string
	}{
		{
			name: "Task query successful",
			setupKV: func(kv nats.KeyValue) {
				key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
				task := &condition.Task[any, any]{
					ID:    uuid.MustParse(conditionID),
					State: condition.Active,
				}
				taskBytes, _ := task.Marshal()
				_, errPut := kv.Put(key, taskBytes)
				require.NoError(t, errPut)
			},
			expectedTask: &condition.Task[any, any]{
				ID:    uuid.MustParse(conditionID),
				State: condition.Active,
			},
		},
		{
			name:               "Task not found",
			setupKV:            func(_ nats.KeyValue) {},
			expectedQueryError: errTaskNotFound.Error(),
		},
		{
			name: "invalid task data",
			setupKV: func(kv nats.KeyValue) {
				key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
				_, errPut := kv.Put(key, []byte("invalid task data"))
				require.NoError(t, errPut)
			},
			expectedInitError: errTaskPublisherInit.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanKV(t, taskKV)
			tt.setupKV(taskKV)

			repo, errInit := NewNatsConditionTaskRepository(
				conditionID,
				serverID,
				facilityCode,
				conditionKind,
				controllerID,
				0,
				njs,
				logger,
			)

			if tt.expectedInitError != "" {
				assert.Error(t, errInit)
				assert.Contains(t, errInit.Error(), tt.expectedInitError)

				return
			}

			assert.NoError(t, errInit)
			assert.NotNil(t, repo)

			task, errQuery := repo.Query(context.Background())
			if tt.expectedQueryError != "" {
				assert.Error(t, errQuery)
				assert.Contains(t, errQuery.Error(), tt.expectedQueryError)
			} else {
				assert.NoError(t, errQuery)
				assert.Equal(t, tt.expectedTask, task)
			}
		})
	}
}

func TestNatsConditionTaskRepository_update(t *testing.T) {
	// Start a NATS server
	ns := runNATSServer(t)
	defer shutdownNATSServer(t, ns)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	require.NoError(t, err)

	// conditionID
	cid := uuid.New()

	// serverID
	sid := uuid.New()

	taskKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: condition.TaskKVRepositoryBucket,
	})

	key := condition.TaskKVRepositoryKey("fac13", condition.FirmwareInstall, sid.String())

	require.NoError(t, err)

	tests := []struct {
		name           string
		setupKV        func(kv nats.KeyValue) uint64
		key            string
		taskUpdate     *condition.Task[any, any]
		onlyTimestamp  bool
		expectedError  string
		expectedResult func(t *testing.T, task *condition.Task[any, any])
	}{
		{
			name: "Successful full update",
			setupKV: func(kv nats.KeyValue) uint64 {
				currentTask := &condition.Task[any, any]{
					ID:        cid,
					State:     condition.Active,
					UpdatedAt: time.Now().Add(-1 * time.Hour),
				}
				currentTaskJSON, _ := currentTask.Marshal()
				rev, err := kv.Put(key, currentTaskJSON)
				require.NoError(t, err, "setupKV")
				return rev
			},
			key: key,
			taskUpdate: &condition.Task[any, any]{
				ID:    cid,
				State: condition.Succeeded,
			},
			onlyTimestamp: false,
			expectedResult: func(t *testing.T, task *condition.Task[any, any]) {
				assert.Equal(t, cid, task.ID)
				assert.Equal(t, condition.Succeeded, task.State)
				assert.WithinDuration(t, time.Now(), task.UpdatedAt, 2*time.Second)
			},
		},
		{
			name: "Successful timestamp-only update",
			setupKV: func(kv nats.KeyValue) uint64 {
				currentTask := &condition.Task[any, any]{
					ID:        cid,
					State:     condition.Active,
					UpdatedAt: time.Now().Add(-1 * time.Hour),
				}
				currentTaskJSON, _ := currentTask.Marshal()
				rev, err := kv.Put(key, currentTaskJSON)
				require.NoError(t, err, "setupKV")
				return rev
			},
			key:           key,
			taskUpdate:    nil,
			onlyTimestamp: true,
			expectedResult: func(t *testing.T, task *condition.Task[any, any]) {
				assert.Equal(t, cid, task.ID)
				assert.Equal(t, condition.Active, task.State)
				assert.WithinDuration(t, time.Now(), task.UpdatedAt, 2*time.Second)
			},
		},
		{
			name:          "Error getting key",
			setupKV:       func(_ nats.KeyValue) uint64 { return 0 },
			key:           "non-existent-key",
			taskUpdate:    nil,
			onlyTimestamp: false,
			expectedError: "key not found",
		},
		{
			name: "Error unmarshaling current task",
			setupKV: func(kv nats.KeyValue) uint64 {
				rev, err := kv.Put(key, []byte("invalid json"))
				require.NoError(t, err)
				return rev
			},
			key:           key,
			taskUpdate:    nil,
			onlyTimestamp: false,
			expectedError: "error unmarshal key",
		},
		{
			name: "Error updating task",
			setupKV: func(kv nats.KeyValue) uint64 {
				currentTask := &condition.Task[any, any]{
					ID:    cid,
					State: condition.Active,
				}
				currentTaskJSON, _ := currentTask.Marshal()
				rev, err := kv.Put(key, currentTaskJSON)
				require.NoError(t, err)
				return rev
			},
			key: key,
			taskUpdate: &condition.Task[any, any]{
				ID:    uuid.New(), // Different ID
				State: "completed",
			},
			onlyTimestamp: false,
			expectedError: "Task.ID mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanKV(t, taskKV)
			rev := tt.setupKV(taskKV)
			repo := &NatsConditionTaskRepository{kv: taskKV, lastRev: rev}

			_, err := repo.update(tt.key, tt.taskUpdate, tt.onlyTimestamp)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)

				// Verify the updated task
				entry, err := taskKV.Get(tt.key)
				require.NoError(t, err)

				updatedTask := &condition.Task[any, any]{}
				err = json.Unmarshal(entry.Value(), updatedTask)
				require.NoError(t, err)

				tt.expectedResult(t, updatedTask)
			}
		})
	}
}

func TestNatsConditionTaskRepository_Publish(t *testing.T) {
	// Start a NATS server
	ns := runNATSServer(t)
	defer shutdownNATSServer(t, ns)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	require.NoError(t, err)

	taskKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: condition.TaskKVRepositoryBucket,
	})
	require.NoError(t, err)

	facilityCode := "area13"
	sid := uuid.New()
	cid := uuid.New()
	conditionKind := condition.FirmwareInstall
	controllerID := "test-controller"

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	tests := []struct {
		name          string
		setupRepo     func() *NatsConditionTaskRepository
		task          *condition.Task[any, any]
		tsUpdateOnly  bool
		expectedError string
	}{
		{
			name: "Successful new task publish",
			setupRepo: func() *NatsConditionTaskRepository {
				return &NatsConditionTaskRepository{
					kv:           taskKV,
					facilityCode: facilityCode,
					serverID:     sid.String(),
					conditionID:  cid.String(),
					controllerID: controllerID,
					log:          logger,
					lastRev:      0,
				}
			},
			task: &condition.Task[any, any]{
				ID:    cid,
				Kind:  conditionKind,
				State: condition.Active,
			},
			tsUpdateOnly: false,
		},
		{
			name: "Successful task update",
			setupRepo: func() *NatsConditionTaskRepository {
				repo := &NatsConditionTaskRepository{
					kv:           taskKV,
					facilityCode: facilityCode,
					serverID:     sid.String(),
					conditionID:  cid.String(),
					controllerID: controllerID,
					log:          logger,
					lastRev:      0,
				}
				initialTask := &condition.Task[any, any]{
					ID:    cid,
					Kind:  conditionKind,
					State: condition.Active,
				}
				err := repo.Publish(context.Background(), initialTask, false)
				require.NoError(t, err)
				return repo
			},
			task: &condition.Task[any, any]{
				ID:    cid,
				Kind:  conditionKind,
				State: condition.Succeeded,
			},
			tsUpdateOnly: false,
		},
		{
			name: "Successful timestamp-only update",
			setupRepo: func() *NatsConditionTaskRepository {
				repo := &NatsConditionTaskRepository{
					kv:           taskKV,
					facilityCode: facilityCode,
					serverID:     sid.String(),
					conditionID:  cid.String(),
					controllerID: controllerID,
					log:          logger,
					lastRev:      0,
				}
				initialTask := &condition.Task[any, any]{
					ID:    cid,
					Kind:  conditionKind,
					State: condition.Active,
				}
				err := repo.Publish(context.Background(), initialTask, false)
				require.NoError(t, err)
				return repo
			},
			task: &condition.Task[any, any]{
				ID:    cid,
				Kind:  conditionKind,
				State: condition.Active,
			},
			tsUpdateOnly: true,
		},
		{
			name: "Error on task marshaling",
			setupRepo: func() *NatsConditionTaskRepository {
				return &NatsConditionTaskRepository{
					kv:           taskKV,
					facilityCode: facilityCode,
					serverID:     sid.String(),
					conditionID:  cid.String(),
					controllerID: controllerID,
					log:          logger,
					lastRev:      0,
				}
			},
			task: &condition.Task[any, any]{
				ID:    cid,
				Kind:  conditionKind,
				State: condition.Active,
				Data:  make(chan int), // borky data
			},
			tsUpdateOnly:  false,
			expectedError: "task publish error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanKV(t, taskKV)
			repo := tt.setupRepo()

			err := repo.Publish(context.Background(), tt.task, tt.tsUpdateOnly)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)

				// verify the published task
				key := condition.TaskKVRepositoryKey(facilityCode, tt.task.Kind, sid.String())
				entry, err := taskKV.Get(key)
				require.NoError(t, err)

				publishedTask := &condition.Task[any, any]{}
				err = json.Unmarshal(entry.Value(), publishedTask)
				require.NoError(t, err)

				assert.Equal(t, tt.task.ID, publishedTask.ID)
				assert.Equal(t, tt.task.Kind, publishedTask.Kind)
				assert.Equal(t, tt.task.State, publishedTask.State)
				if tt.tsUpdateOnly {
					assert.WithinDuration(t, time.Now(), publishedTask.UpdatedAt, 2*time.Second)
				} else {
					assert.Equal(t, tt.task.State, publishedTask.State)
				}
			}
		})
	}
}

func TestHTTPTaskRepository_Publish(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	serverID := uuid.New()
	conditionID := uuid.New()
	conditionKind := condition.FirmwareInstall

	repo := &HTTPTaskRepository{
		controllerID:  registry.GetIDWithUUID("test", serverID),
		conditionID:   conditionID,
		conditionKind: conditionKind,
		serverID:      serverID,
		logger:        logger,
	}

	tests := []struct {
		name          string
		task          *condition.Task[any, any]
		tsUpdateOnly  bool
		mockSetup     func(m *orc.MockQueryor)
		expectedError string
	}{
		{
			name: "Successful publish",
			task: &condition.Task[any, any]{
				ID:    conditionID,
				State: condition.Active,
			},
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskPublish",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					mock.MatchedBy(
						func(t *condition.Task[any, any]) bool {
							return t.ID == conditionID && t.State == condition.Active
						},
					),
					false,
				).Return(&orctypes.ServerResponse{StatusCode: 200}, nil)
			},
		},
		{
			name: "Successful timestamp-only update",
			task: &condition.Task[any, any]{
				ID:    conditionID,
				State: condition.Active,
			},
			tsUpdateOnly: true,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskPublish",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					mock.IsType(&condition.Task[any, any]{}),
					true,
				).Return(&orctypes.ServerResponse{StatusCode: 200}, nil)
			},
		},
		{
			name: "Publish error",
			task: &condition.Task[any, any]{
				ID:    conditionID,
				State: condition.Active,
			},
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskPublish",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					mock.IsType(&condition.Task[any, any]{}),
					false,
				).Return(nil, errors.New("Publish error"))
			},
			expectedError: "Publish error",
		},
		{
			name: "Non-200 status code",
			task: &condition.Task[any, any]{
				ID:    conditionID,
				State: condition.Active,
			},
			tsUpdateOnly: false,
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskPublish",
					mock.Anything,
					conditionKind,
					serverID,
					conditionID,
					mock.IsType(&condition.Task[any, any]{}),
					false,
				).Return(&orctypes.ServerResponse{StatusCode: 400}, nil)
			},
			expectedError: "API Query returned error, status code: 400: task publish error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryor := new(orc.MockQueryor)
			tt.mockSetup(mockQueryor)
			repo.orcQueryor = mockQueryor

			err := repo.Publish(context.Background(), tt.task, tt.tsUpdateOnly)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHTTPTaskRepository_Query(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	serverID := uuid.New()
	conditionID := uuid.New()
	conditionKind := condition.FirmwareInstall

	repo := &HTTPTaskRepository{
		conditionID:   conditionID,
		conditionKind: conditionKind,
		serverID:      serverID,
		logger:        logger,
	}

	tests := []struct {
		name          string
		mockSetup     func(m *orc.MockQueryor)
		expectedTask  *condition.Task[any, any]
		expectedError string
	}{
		{
			name: "Successful query",
			mockSetup: func(m *orc.MockQueryor) {
				expectedTask := &condition.Task[any, any]{
					ID:    conditionID,
					State: condition.Active,
				}
				m.On("ConditionTaskQuery",
					mock.Anything,
					conditionKind,
					serverID,
				).Return(&orctypes.ServerResponse{
					StatusCode: 200,
					Task:       expectedTask,
				}, nil)
			},
			expectedTask: &condition.Task[any, any]{
				ID:    conditionID,
				State: condition.Active,
			},
		},
		{
			name: "Query error",
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery",
					mock.Anything,
					conditionKind,
					serverID,
				).Return(nil, errors.New("Task query error"))
			},
			expectedError: "Task query error",
		},
		{
			name: "No task in response",
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery",
					mock.Anything,
					conditionKind,
					serverID,
				).Return(&orctypes.ServerResponse{
					StatusCode: 200,
					Task:       nil,
				}, nil)
			},
			expectedError: "no Task object in response",
		},
		{
			name: "Non-200 status code",
			mockSetup: func(m *orc.MockQueryor) {
				m.On("ConditionTaskQuery",
					mock.Anything,
					conditionKind,
					serverID,
				).Return(&orctypes.ServerResponse{
					StatusCode: 404,
					Message:    "Not Found",
				}, nil)
			},
			expectedError: "API Query returned error, status code: 404, msg: Not Found: task query error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryor := new(orc.MockQueryor)
			tt.mockSetup(mockQueryor)
			repo.orcQueryor = mockQueryor

			task, err := repo.Query(context.Background())

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, task)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, task)
				assert.Equal(t, tt.expectedTask.ID, task.ID)
				assert.Equal(t, tt.expectedTask.State, task.State)
			}
		})
	}
}
