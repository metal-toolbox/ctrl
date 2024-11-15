package ctrl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/metal-toolbox/rivets/v2/condition"
	"github.com/metal-toolbox/rivets/v2/events"
	"github.com/metal-toolbox/rivets/v2/events/pkg/kv"
	"github.com/metal-toolbox/rivets/v2/events/registry"

	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
)

var (
	kvTTL            = 10 * 24 * time.Hour
	errKV            = errors.New("unable to bind to status KV bucket")
	errGetKey        = errors.New("error fetching existing key, value for update")
	errUnmarshalKey  = errors.New("error unmarshal key, value for update")
	errStatusValue   = errors.New("condition status value error")
	errStatusPublish = errors.New("condition status publish error")
)

// ConditionStatusPublisher defines an interface for publishing status updates for conditions.
type ConditionStatusPublisher interface {
	Publish(ctx context.Context, serverID string, state condition.State, status json.RawMessage, tsUpdateOnly bool) error
}

// NatsConditionStatusPublisher implements the StatusPublisher interface to publish condition status updates using NATS.
type NatsConditionStatusPublisher struct {
	kv           nats.KeyValue
	log          *logrus.Logger
	facilityCode string
	conditionID  string
	controllerID registry.ControllerID
}

// NewNatsConditionStatusPublisher creates a new NatsConditionStatusPublisher for a given condition ID.
//
// It initializes a NATS KeyValue store for tracking condition statuses.
func NewNatsConditionStatusPublisher(
	appName,
	conditionID,
	facilityCode string,
	conditionKind condition.Kind,
	controllerID registry.ControllerID,
	kvReplicas int,
	stream *events.NatsJetstream,
	logger *logrus.Logger,
) (ConditionStatusPublisher, error) {
	kvOpts := []kv.Option{
		kv.WithDescription(fmt.Sprintf("%s condition status tracking", appName)),
		kv.WithTTL(kvTTL),
		kv.WithReplicas(kvReplicas),
	}

	statusKV, err := kv.CreateOrBindKVBucket(stream, string(conditionKind), kvOpts...)
	if err != nil {
		return nil, errors.Wrap(errKV, err.Error())
	}

	return &NatsConditionStatusPublisher{
		facilityCode: facilityCode,
		conditionID:  conditionID,
		controllerID: controllerID,
		kv:           statusKV,
		log:          logger,
	}, nil
}

// Publish implements the StatusPublisher interface. It serializes and publishes the current status of a condition to NATS.
func (s *NatsConditionStatusPublisher) Publish(ctx context.Context, serverID string, state condition.State, status json.RawMessage, _ bool) error {
	_, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.Publish.KV",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	sv := &condition.StatusValue{
		WorkerID:  s.controllerID.String(),
		Target:    serverID,
		TraceID:   trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		SpanID:    trace.SpanFromContext(ctx).SpanContext().SpanID().String(),
		State:     string(state),
		Status:    status,
		UpdatedAt: time.Now(),
	}

	key := condition.StatusValueKVKey(s.facilityCode, s.conditionID)

	err := s.update(key, sv)
	if err != nil {
		metricsNATSError("publish-condition-status")
		span.AddEvent("status publish failure",
			trace.WithAttributes(
				attribute.String("controllerID", s.controllerID.String()),
				attribute.String("serverID", serverID),
				attribute.String("conditionID", s.conditionID),
				attribute.String("error", err.Error()),
			),
		)

		s.log.WithError(err).WithFields(logrus.Fields{
			"serverID":          serverID,
			"assetFacilityCode": s.facilityCode,
			"conditionID":       s.conditionID,
			"controllerID":      s.controllerID,
			"key":               key,
		}).Warn("Condition status publish failed")

		return errors.Wrap(errStatusPublish, err.Error())
	}

	s.log.WithFields(logrus.Fields{
		"serverID":          serverID,
		"assetFacilityCode": s.facilityCode,
		"taskID":            s.conditionID,
		"key":               key,
	}).Trace("Condition status published")

	return nil
}

func (s *NatsConditionStatusPublisher) update(key string, sv *condition.StatusValue) error {
	curSV := &condition.StatusValue{}
	// fetch current status value from KV
	entry, err := s.kv.Get(key)
	switch err {
	case nats.ErrKeyNotFound:
		// create a KV entry for this status value
		sv.CreatedAt = sv.UpdatedAt // we set UpdatedAt in the body of Publish above
		_, err = s.kv.Create(key, sv.MustBytes())
		return err
	case nil:
		// we found something under that key, update it
		if errJSON := json.Unmarshal(entry.Value(), curSV); errJSON != nil {
			return errors.Wrap(errUnmarshalKey, errJSON.Error())
		}
		// don't update a completed condition
		if condition.StateIsComplete(condition.State(curSV.State)) {
			return fmt.Errorf("%w: attempt to update a completed condition", errStatusValue)
		}
		// update the KV with the new value
		sv.CreatedAt = curSV.CreatedAt
		_, err = s.kv.Update(key, sv.MustBytes(), entry.Revision())
		return err
	default:
		return errors.Wrap(errGetKey, err.Error())
	}
}

// ConditionState represents the various states a condition can be in during its lifecycle.
type ConditionState int

const (
	notStarted    ConditionState = iota
	inProgress                   // another controller has started it, is still around and updated recently
	complete                     // condition is done
	orphaned                     // the controller that started this task doesn't exist anymore
	indeterminate                // we got an error in the process of making the check
)

// ConditionStatusQueryor defines an interface for querying the status of a condition.
type ConditionStatusQueryor interface {
	// ConditionState returns the current state of a condition based on its ID.
	ConditionState(conditionID string) ConditionState
}

// NatsConditionStatusQueryor implements ConditionStatusQueryor to query condition states using NATS.
type NatsConditionStatusQueryor struct {
	kv           nats.KeyValue
	logger       *logrus.Logger
	facilityCode string
	controllerID string
}

// NewNatsConditionStatusQueryor creates a new NatsConditionStatusQueryor instance, initializing a NATS KeyValue store for condition status queries.
func (n *NatsController) NewNatsConditionStatusQueryor() (*NatsConditionStatusQueryor, error) {
	errKV := errors.New("unable to connect to status KV for condition progress lookup")
	kvHandle, err := events.AsNatsJetStreamContext(n.stream.(*events.NatsJetstream)).KeyValue(string(n.conditionKind))
	if err != nil {
		n.logger.WithError(err).Error(errKV.Error())
		return nil, errors.Wrap(errKV, err.Error())
	}

	return &NatsConditionStatusQueryor{
		kv:           kvHandle,
		logger:       n.logger,
		facilityCode: n.facilityCode,
		controllerID: n.ID(),
	}, nil
}

// ConditionState queries the NATS KeyValue store to determine the current state of a condition.
func (p *NatsConditionStatusQueryor) ConditionState(conditionID string) ConditionState {
	key := condition.StatusValueKVKey(p.facilityCode, conditionID)
	entry, err := p.kv.Get(key)
	switch err {
	case nats.ErrKeyNotFound:
		// This should be by far the most common path through this code.
		return notStarted
	case nil:
		break // we'll handle this outside the switch
	default:
		p.logger.WithError(err).WithFields(logrus.Fields{
			"conditionID": conditionID,
			"key":         key,
		}).Warn("error reading condition status")

		return indeterminate
	}

	// we have an status entry for this condition, is is complete?
	sv := condition.StatusValue{}
	if errJSON := json.Unmarshal(entry.Value(), &sv); errJSON != nil {
		p.logger.WithError(errJSON).WithFields(logrus.Fields{
			"conditionID": conditionID,
			"lookupKey":   key,
		}).Warn("unable to construct a sane status for this condition")

		return indeterminate
	}

	if condition.State(sv.State) == condition.Failed ||
		condition.State(sv.State) == condition.Succeeded {
		p.logger.WithFields(logrus.Fields{
			"conditionID":    conditionID,
			"conditionState": sv.State,
			"lookupKey":      key,
		}).Info("this condition is already complete")

		return complete
	}

	// is the worker handling this condition alive?
	worker, err := registry.ControllerIDFromString(sv.WorkerID)
	if err != nil {
		p.logger.WithError(err).WithFields(logrus.Fields{
			"conditionID":           conditionID,
			"original controllerID": sv.WorkerID,
		}).Warn("bad controller identifier")

		return indeterminate
	}

	activeAt, err := registry.LastContact(worker)
	switch err {
	case nats.ErrKeyNotFound:
		// the data for this worker aged-out, it's no longer active
		// XXX: the most conservative thing to do here is to return
		// indeterminate but most times this will indicate that the
		// worker crashed/restarted and this task should be restarted.
		p.logger.WithFields(logrus.Fields{
			"conditionID":           conditionID,
			"original controllerID": sv.WorkerID,
		}).Info("original controller not found")

		// We're going to restart this condition when we return from
		// this function. Use the KV handle we have to delete the
		// existing task key.
		if err = p.kv.Delete(key); err != nil {
			p.logger.WithError(err).WithFields(logrus.Fields{
				"conditionID":           conditionID,
				"original controllerID": sv.WorkerID,
				"lookupKey":             key,
			}).Warn("unable to delete existing condition status")

			return indeterminate
		}

		return orphaned
	case nil:
		timeStr, _ := activeAt.MarshalText()
		p.logger.WithError(err).WithFields(logrus.Fields{
			"conditionID":           conditionID,
			"original controllerID": sv.WorkerID,
			"lastActive":            timeStr,
		}).Warn("error looking up controller last contact")

		return inProgress
	default:
		p.logger.WithError(err).WithFields(logrus.Fields{
			"conditionID":           conditionID,
			"original controllerID": sv.WorkerID,
		}).Warn("error looking up controller last contact")

		return indeterminate
	}
}

// eventStatusAcknowleger provides an interface for acknowledging the status of events in a NATS JetStream.
type eventStatusAcknowleger interface {
	// inProgress marks the event as being in progress in the NATS JetStream.
	inProgress()
	// complete marks the event as complete in the NATS JetStream.
	complete()
	// nak sends a negative acknowledgment for the event in the NATS JetStream, indicating it requires further handling.
	nak()
}

// natsEventStatusAcknowleger implements eventStatusAcknowleger to interact with NATS JetStream events.
type natsEventStatusAcknowleger struct {
	event  events.Message
	logger *logrus.Logger
}

func (n *NatsController) newNatsEventStatusAcknowleger(event events.Message) *natsEventStatusAcknowleger {
	return &natsEventStatusAcknowleger{event, n.logger}
}

// inProgress marks the event as being in progress in the NATS JetStream.
func (p *natsEventStatusAcknowleger) inProgress() {
	if err := p.event.InProgress(); err != nil {
		metricsNATSError("ack-in-progress")
		p.logger.WithError(err).Warn("event Ack as Inprogress returned error")
		return
	}

	p.logger.Trace("event ack as InProgress successful")
}

// complete marks the event as complete in the NATS JetStream.
func (p *natsEventStatusAcknowleger) complete() {
	if err := p.event.Ack(); err != nil {
		metricsNATSError("ack")
		p.logger.WithError(err).Warn("event Ack as complete returned error")
		return
	}

	p.logger.Trace("event ack as Complete successful")
}

// nak sends a negative acknowledgment for the event in the NATS JetStream, indicating it requires further handling.
func (p *natsEventStatusAcknowleger) nak() {
	if err := p.event.Nak(); err != nil {
		metricsNATSError("nak")
		p.logger.WithError(err).Warn("event Nak error")
		return
	}

	p.logger.Trace("event nak successful")
}

// HTTPConditionStatusPublisher implements the StatusPublisher interface to publish condition status updates over HTTP to NATS.
type HTTPConditionStatusPublisher struct {
	logger        *logrus.Logger
	orcQueryor    orc.Queryor
	conditionKind condition.Kind
	controllerID  registry.ControllerID
	conditionID   uuid.UUID
	serverID      uuid.UUID
}

func NewHTTPConditionStatusPublisher(
	appName string,
	serverID,
	conditionID uuid.UUID,
	conditionKind condition.Kind,
	orcQueryor orc.Queryor,
	logger *logrus.Logger,
) ConditionStatusPublisher {
	return &HTTPConditionStatusPublisher{
		controllerID:  registry.GetIDWithUUID(appName, serverID),
		orcQueryor:    orcQueryor,
		conditionID:   conditionID,
		conditionKind: conditionKind,
		serverID:      serverID,
		logger:        logger,
	}
}

func (s *HTTPConditionStatusPublisher) Publish(ctx context.Context, serverID string, state condition.State, status json.RawMessage, tsUpdateOnly bool) error {
	octx, span := otel.Tracer(pkgName).Start(
		ctx,
		"controller.status_http.Publish",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	sv := &condition.StatusValue{
		WorkerID:  s.controllerID.String(),
		Target:    serverID,
		TraceID:   trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		SpanID:    trace.SpanFromContext(ctx).SpanContext().SpanID().String(),
		State:     string(state),
		Status:    status,
		UpdatedAt: time.Now(),
	}

	resp, err := s.orcQueryor.ConditionStatusUpdate(octx, s.conditionKind, s.serverID, s.conditionID, sv, tsUpdateOnly)
	if err != nil {
		s.logger.WithError(err).Error("condition status update error")
		return err
	}

	if resp.StatusCode != 200 {
		err := newQueryError(resp.StatusCode, resp.Message)
		s.logger.WithError(errStatusPublish).Error(err)
		return errors.Wrap(errStatusPublish, err.Error())
	}

	s.logger.WithFields(
		logrus.Fields{
			"status":       resp.StatusCode,
			"controllerID": sv.WorkerID,
		},
	).Trace("condition status update published successfully")

	return nil
}
