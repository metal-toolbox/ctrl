package ctrl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/metal-toolbox/rivets/condition"
	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/pkg/kv"
	"github.com/metal-toolbox/rivets/events/registry"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
)

var (
	kvTaskTTL            = 10 * 24 * time.Hour
	errTaskPublish       = errors.New("task publish error")
	errTaskNotFound      = errors.New("task not found")
	errTaskQuery         = errors.New("task query error")
	errTaskPublisherInit = errors.New("task publisher init error")
)

// ConditionTaskRepository defines an interface for storing and querying Task information for a condition.
type ConditionTaskRepository interface {
	Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error
	Query(ctx context.Context) (*condition.Task[any, any], error)
}

type NatsConditionTaskRepository struct {
	kv            nats.KeyValue
	log           *logrus.Logger
	facilityCode  string
	serverID      string
	conditionID   string
	conditionKind condition.Kind
	controllerID  string
	lastRev       uint64
	bucketName    string
}

// Returns a NatsConditionTaskRepository to store a retrieve Task information for a condition.
//
// It initializes a NATS KeyValue store for storing Task information for conditions.
// The conditionID is the TaskID.
func NewNatsConditionTaskRepository(
	conditionID,
	serverID,
	facilityCode string,
	conditionKind condition.Kind,
	controllerID registry.ControllerID,
	kvReplicas int,
	stream *events.NatsJetstream,
	logger *logrus.Logger,
) (ConditionTaskRepository, error) {
	kvOpts := []kv.Option{
		kv.WithDescription("Condition task repository"),
		kv.WithTTL(kvTaskTTL),
		kv.WithReplicas(kvReplicas),
	}

	repo := &NatsConditionTaskRepository{
		facilityCode:  facilityCode,
		conditionID:   conditionID,
		conditionKind: conditionKind,
		controllerID:  controllerID.String(),
		serverID:      serverID,
		bucketName:    condition.TaskKVRepositoryBucket,
		log:           logger,
	}

	errKV := errors.New("unable to bind to task KV bucket: " + repo.bucketName)
	var err error
	repo.kv, err = kv.CreateOrBindKVBucket(stream, repo.bucketName, kvOpts...)
	if err != nil {
		return nil, errors.Wrap(errKV, err.Error())
	}

	// retrieve current key revision if key exists
	key := condition.TaskKVRepositoryKey(facilityCode, conditionKind, serverID)
	currTaskEntry, errGet := repo.kv.Get(key)
	if errGet != nil && !errors.Is(errGet, nats.ErrKeyNotFound) {
		return nil, errors.Wrap(
			errTaskPublisherInit,
			fmt.Sprintf("key: %s, error: %s", key, errGetKey.Error()),
		)
	}

	// verify existing TaskID matches ConditionID and task is not active
	if currTaskEntry != nil {
		repo.lastRev = currTaskEntry.Revision()
		currTask, err := condition.TaskFromMessage(currTaskEntry.Value())
		if err != nil {
			return nil, errors.Wrap(
				errTaskPublisherInit,
				"unable to deserialize current Task object: "+err.Error(),
			)
		}

		if currTask.ID.String() != conditionID && !condition.StateIsComplete(currTask.State) {
			msg := fmt.Sprintf(
				"existing Task object %s in in-complete state, does not match ConditionID: %s, must be purged before proceeding",
				currTask.ID.String(),
				conditionID,
			)

			return nil, errors.Wrap(
				errTaskPublisherInit,
				msg,
			)
		}

		// current task object is in completed state, purge.
		if condition.StateIsComplete(currTask.State) {
			if err := repo.kv.Delete(key); err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
				return nil, errors.Wrap(
					errTaskPublisherInit,
					fmt.Sprintf("key: %s, error: %s", key, err.Error()),
				)
			}
		}
	}

	return repo, nil
}

// Publish implements the ConditionTaskRepository interface
func (n *NatsConditionTaskRepository) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error {
	_, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.Publish.KV.Task",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	key := condition.TaskKVRepositoryKey(n.facilityCode, task.Kind, n.serverID)
	failed := func(err error) error {
		metricsNATSError("publish-task")
		span.AddEvent("Task publish failure",
			trace.WithAttributes(
				attribute.String("controllerID", n.controllerID),
				attribute.String("serverID", n.serverID),
				attribute.String("conditionID", n.conditionID),
				attribute.String("error", err.Error()),
			),
		)

		n.log.WithError(err).WithFields(logrus.Fields{
			"serverID":     n.serverID,
			"facilityCode": n.facilityCode,
			"conditionID":  n.conditionID,
			"lastRev":      n.lastRev,
			"controllerID": n.controllerID,
			"key":          key,
		}).Warn("Task publish failure")

		return errors.Wrap(errTaskPublish, err.Error())
	}

	var err error
	var rev uint64
	if n.lastRev == 0 {
		task.CreatedAt = time.Now()

		taskJSON, errMarshal := task.Marshal()
		if errMarshal != nil {
			return failed(errMarshal)
		}

		rev, err = n.kv.Create(key, taskJSON)
	} else {
		rev, err = n.update(key, task, tsUpdateOnly)
	}

	if err != nil {
		return failed(err)
	}

	n.lastRev = rev
	n.log.WithFields(logrus.Fields{
		"serverID":     n.serverID,
		"facilityCode": n.facilityCode,
		"taskID":       n.conditionID,
		"lastRev":      n.lastRev,
		"key":          key,
	}).Trace("Task published")

	return nil
}

func (n *NatsConditionTaskRepository) update(key string, newTaskValue *condition.Task[any, any], onlyTimestamp bool) (uint64, error) {
	// fetch current status value from KV
	entry, err := n.kv.Get(key)
	if err != nil {
		return 0, errors.Wrap(errGetKey, err.Error())
	}

	currTaskObj := &condition.Task[any, any]{}
	if errJSON := json.Unmarshal(entry.Value(), &currTaskObj); errJSON != nil {
		return 0, errors.Wrap(errUnmarshalKey, errJSON.Error())
	}

	var update *condition.Task[any, any]
	if onlyTimestamp {
		// timestamp only update
		currTaskObj.UpdatedAt = time.Now()
	} else {
		// full status update
		err = currTaskObj.Update(newTaskValue)
		if err != nil {
			return 0, err
		}
	}

	update = currTaskObj

	taskJSON, err := update.Marshal()
	if err != nil {
		return 0, err
	}

	rev, err := n.kv.Update(key, taskJSON, n.lastRev)
	if err != nil {
		return 0, err
	}

	return rev, nil
}

// Query implements the ConditionTaskRepository interface to return the Task for a Condition
func (n *NatsConditionTaskRepository) Query(ctx context.Context) (*condition.Task[any, any], error) {
	_, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.Query.KV.Task",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	key := condition.TaskKVRepositoryKey(n.facilityCode, n.conditionKind, n.serverID)
	failed := func(err error) (*condition.Task[any, any], error) {
		metricsNATSError("query-task")
		span.AddEvent("Task query failure",
			trace.WithAttributes(
				attribute.String("controllerID", n.controllerID),
				attribute.String("serverID", n.serverID),
				attribute.String("conditionID", n.conditionID),
				attribute.String("error", err.Error()),
			),
		)

		n.log.WithError(err).WithFields(logrus.Fields{
			"serverID":     n.serverID,
			"facilityCode": n.facilityCode,
			"conditionID":  n.conditionID,
			"lastRev":      n.lastRev,
			"controllerID": n.controllerID,
			"key":          key,
		}).Warn("Task query failure")

		return nil, err
	}

	kventry, err := n.kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return failed(errors.Wrap(errTaskNotFound, err.Error()))
		}
		return failed(errors.Wrap(errTaskQuery, err.Error()))
	}

	task, err := condition.TaskFromMessage(kventry.Value())
	if err != nil {
		return failed(errors.Wrap(errTaskQuery, err.Error()))
	}

	return task, nil
}

// HTTPTaskRepository implements the ConditionTaskRepository to Publish and Query Task information from NATS over HTTP.
type HTTPTaskRepository struct {
	logger        *logrus.Logger
	orcQueryor    orc.Queryor
	conditionKind condition.Kind
	conditionID   uuid.UUID
	serverID      uuid.UUID
}

func NewHTTPTaskRepository(
	serverID,
	conditionID uuid.UUID,
	conditionKind condition.Kind,
	orcQueryor orc.Queryor,
	logger *logrus.Logger,
) ConditionTaskRepository {
	return &HTTPTaskRepository{
		orcQueryor:    orcQueryor,
		conditionID:   conditionID,
		conditionKind: conditionKind,
		serverID:      serverID,
		logger:        logger,
	}
}

// Publish implements the ConditionTaskRepository interface to record Task information in the Task KV.
func (h *HTTPTaskRepository) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error {
	_, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.task_http.Publish",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	resp, err := h.orcQueryor.ConditionTaskPublish(ctx, h.conditionKind, h.serverID, h.conditionID, task, tsUpdateOnly)
	if err != nil {
		h.logger.WithError(err).Error("Task update error")
		return errors.Wrap(errTaskPublish, err.Error())
	}

	if resp.StatusCode != 200 {
		err := newQueryError(resp.StatusCode, resp.Message)
		h.logger.WithError(errTaskPublish).Error(err)
		return errors.Wrap(errTaskPublish, err.Error())
	}

	h.logger.WithFields(
		logrus.Fields{
			"status": resp.StatusCode,
		},
	).Trace("Task update published successfully")

	return nil
}

// Query implements the ConditionTaskRepository interface to retrieve Task information from the Task KV.
func (h *HTTPTaskRepository) Query(ctx context.Context) (*condition.Task[any, any], error) {
	errNoTask := errors.New("no Task object in response")

	_, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.task_http.Query",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	resp, err := h.orcQueryor.ConditionTaskQuery(ctx, h.conditionKind, h.serverID)
	if err != nil {
		h.logger.WithError(err).Error("Task query error")
		return nil, errors.Wrap(errTaskQuery, err.Error())
	}

	if resp.StatusCode != 200 {
		err := newQueryError(resp.StatusCode, resp.Message)
		h.logger.WithError(errTaskQuery).Error(err)
		return nil, errors.Wrap(errTaskQuery, err.Error())
	}

	h.logger.WithFields(
		logrus.Fields{
			"status": resp.StatusCode,
		},
	).Trace("Task queried successfully")

	if resp.Task == nil {
		return nil, errNoTask
	}

	return resp.Task, nil
}
