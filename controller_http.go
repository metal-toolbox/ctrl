package controller

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
	"github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/types"
	"github.com/metal-toolbox/rivets/condition"
	"github.com/metal-toolbox/rivets/events/registry"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	pkgNameHttpController = "events/httpcontroller"
	// count of times to retry a query (300 * 30 = 2.5h is how long we'll keep trying)
	queryRetries = 300
	// query timeout duration
	queryTimeout = 10 * time.Second
	// query interval duration
	queryInterval = 30 * time.Second
)

var (
	ErrHandlerInit   = errors.New("error initializing handler")
	ErrEmptyResponse = errors.New("empty response with error")
	ErrRetryRequest  = errors.New("request retry required")
	ErrNoCondition   = errors.New("no condition available")
	ErrNoWork        = errors.New("no Condition or Task identified for work")
)

// NatsHttpController implements the TaskHandler interface to interact with the NATS queue, KV over HTTP(s)
type NatsHttpController struct {
	logger          *logrus.Logger
	facilityCode    string
	serverID        uuid.UUID
	conditionKind   condition.Kind
	queryRetries    int
	queryTimeout    time.Duration
	queryInterval   time.Duration
	handlerTimeout  time.Duration
	checkinInterval time.Duration
	liveness        LivenessCheckin
	orcQueryor      orc.Queryor
}

type OrchestratorApiConfig struct {
	AuthDisabled bool
	Endpoint     string
	AuthToken    string
}

// OptionNatsHttp sets parameters on the NatsHttpController
type OptionNatsHttp func(*NatsHttpController)

func NewNatsHTTPController(
	facilityCode string,
	serverID uuid.UUID,
	conditionKind condition.Kind,
	coClientCfg *OrchestratorApiConfig,
	options ...OptionNatsHttp) (*NatsHttpController, error) {

	logger := logrus.New()
	logger.Formatter = &logrus.JSONFormatter{}

	nhc := &NatsHttpController{
		facilityCode:  facilityCode,
		serverID:      serverID,
		conditionKind: conditionKind,
		queryRetries:  queryRetries,
		queryInterval: queryInterval,
		logger:        logger,
	}

	for _, opt := range options {
		opt(nhc)
	}

	if nhc.orcQueryor == nil {
		orcQueryor, err := newConditionsAPIClient(coClientCfg.Endpoint, coClientCfg.AuthToken, true)
		if err != nil {
			return nil, errors.Wrap(ErrHandlerInit, "error in Conditions API client init: "+err.Error())
		}

		nhc.orcQueryor = orcQueryor
	}

	return nhc, nil
}

func newConditionsAPIClient(endpoint, token string, disableAuth bool) (orc.Queryor, error) {
	if disableAuth {
		return orc.NewClient(
			endpoint,
		)
	}

	// TODO: oauth client magic
	return orc.NewClient(
		endpoint,
		orc.WithAuthToken(token),
	)
}

func WithNatsHttpLogger(logger *logrus.Logger) OptionNatsHttp {
	return func(n *NatsHttpController) {
		n.logger = logger
	}
}

func WithOrchestratorClient(c orc.Queryor) OptionNatsHttp {
	return func(n *NatsHttpController) {
		n.orcQueryor = c
	}
}

func (n *NatsHttpController) traceSpaceContextFromValues(traceID, spanID string) (trace.SpanContext, error) {
	// extract traceID and spanID
	pTraceID, _ := trace.TraceIDFromHex(traceID)
	pSpanID, _ := trace.SpanIDFromHex(spanID)

	// add a trace span
	if pTraceID.IsValid() && pSpanID.IsValid() {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    pTraceID,
			SpanID:     pSpanID,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		}), nil
	}

	errExtract := errors.New("unable to extract span context")
	return trace.SpanContext{}, errExtract
}

func (n *NatsHttpController) ID() string {
	if n.liveness == nil {
		return ""
	}

	return n.liveness.ControllerID().String()
}

func (n *NatsHttpController) Run(ctx context.Context, handler TaskHandler) error {
	ctx, span := otel.Tracer(pkgNameHttpController).Start(
		ctx,
		"Run",
	)
	defer span.End()

	var err error

	task, err := n.fetchWorkWithRetries(ctx, n.serverID, n.queryRetries, n.queryInterval)
	if err != nil {
		return errors.Wrap(ErrHandlerInit, err.Error())
	}

	// set remote span context
	remoteSpanCtx, err := n.traceSpaceContextFromValues(task.TraceID, task.SpanID)
	if err != nil {
		n.logger.Warn(err.Error())
	} else {
		// overwrite span context with remote span when available
		var span trace.Span
		ctx, span = otel.Tracer(pkgNameHttpController).Start(
			trace.ContextWithRemoteSpanContext(ctx, remoteSpanCtx),
			"Run",
		)
		defer span.End()
	}

	return n.runTaskWithMonitor(ctx, handler, task)
}

func (n *NatsHttpController) runTaskWithMonitor(
	ctx context.Context,
	handler TaskHandler,
	task *condition.Task[any, any],
) error {
	ctx, span := otel.Tracer(pkgNameHttpController).Start(
		ctx,
		"runWithMonitor",
	)
	defer span.End()
	// doneCh indicates the handler run completed
	doneCh := make(chan bool)

	// parse controller ID from Task
	controllerID, err := registry.ControllerIDFromString(task.WorkerID)
	if err != nil {
		return errors.Wrap(ErrHandlerInit, err.Error())
	}

	// start controller liveness
	n.liveness = NewHttpNatsLiveness(n.orcQueryor, task.ID, n.serverID, controllerID, statusInterval, n.logger)
	go n.liveness.StartLivenessCheckin(ctx)

	// init publisher
	publisher := NewHTTPPublisher(n.serverID, task.ID, n.conditionKind, controllerID, n.orcQueryor, n.logger)

	// monitor updates TS on status until the task handler returns.
	monitor := func() {
		ticker := time.NewTicker(statusInterval)
		defer ticker.Stop()

		// periodically update the LastUpdate TS in status KV,
		/// which keeps the Orchestrator from reconciling this condition.
	Loop:
		for {
			select {
			case <-ticker.C:
				publisher.Publish(ctx, task, true)
			case <-doneCh:
				break Loop
			}
		}
	}

	go monitor()
	defer close(doneCh)

	logger := n.logger.WithFields(
		logrus.Fields{
			"taskID":   task.ID,
			"state":    task.State,
			"serverID": task.Server.ID,
			"kind":     task.Kind,
		},
	)

	publish := func(state condition.State, status string) {

		// append to existing status record, unless it was overwritten by the controller somehow
		task.Status.Append(status)
		task.State = state

		// publish failed state, status
		if err := publisher.Publish(
			ctx,
			task,
			false,
		); err != nil {
			logger.WithError(err).Error("failed to publish final status")
		}
	}

	// panic handler
	defer func() {
		if rec := recover(); rec != nil {
			// overwrite returned err - declared in func signature
			err := errors.New("Panic occurred while running Condition handler")
			logger.Printf("!!panic %s: %s", rec, debug.Stack())
			logger.Error(err)
			publish(condition.Failed, "Fatal error occurred, check logs for details")
		}
	}() // nolint:errcheck // nope

	if task.State == condition.Pending {
		task.State = condition.Active
		publish(condition.Active, "controller initialized")
	}

	task.Status.Append("controller initialized")
	logger.Info("Controller initialized, running task..")

	if err := handler.HandleTask(ctx, task, publisher); err != nil {
		task.Status.Append("controller returned error: " + err.Error())
		task.State = condition.Failed

		msg := "Controller returned error: " + err.Error()
		logger.Error(msg)
		publish(condition.Failed, msg)
	}

	// TODO:
	// If the handler has returned and not updated the Task.State, StatusValue.State
	// into a final state, then set those fields to failed.
	logger.Info("Controller completed task")

	return nil
}

func sleepWithContext(ctx context.Context, t time.Duration) error {
	select {
	case <-time.After(t):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fetchWorkWithRetries attempts to fetch the Condition and Task objects.
//
// If the Condition object is available that indicates this is first request, because the Condition is pop'ed from the queue.
// if the controller restarts, it will only have access to the Task object going ahead.
//
// The Task object is the runtime information for a Condition, and its available as long as the Task is in an incomplete state.
func (n *NatsHttpController) fetchWorkWithRetries(ctx context.Context, serverID uuid.UUID, tries int, interval time.Duration) (*condition.Task[any, any], error) {
	var task *condition.Task[any, any]
	var cond *condition.Condition

	for attempt := 0; attempt <= tries; attempt++ {
		var err error
		le := n.logger.WithField("attempt", fmt.Sprintf("%d/%d", attempt, tries))

		if attempt > 0 {
			// returns error on context cancellation
			if errSleep := sleepWithContext(ctx, interval); errSleep != nil {
				return nil, errSleep
			}
		}

		if cond == nil {
			// fetch condition
			le.Info("Fetching Condition..")
			cond, err = n.fetchCondition(ctx, serverID)
			if err != nil {
				le.WithError(err).Warn("Condition fetch error")
			} else {
				le.WithFields(logrus.Fields{"ID": cond.ID, "Kind": cond.Kind}).Info("Condition retrieved")
			}
		} else {
			le.WithField("condition.id", cond.ID).Info("Condition was fetched")
		}

		le.Info("Fetching Task..")
		// fetch task and don't proceed until that is available.
		task, err = n.fetchTask(ctx, serverID)
		if err != nil {
			le.WithError(err).Info("retrying...")
			continue
		}

		le.WithFields(logrus.Fields{"id": task.ID, "kind": task.Kind, "state": task.State}).Info("Condition Task retrieved")
		break
	}

	// max attempts reached, no task or condition retrieved
	if cond == nil && task == nil {
		return nil, ErrNoWork
	}

	return task, nil
}

// first attempt to fetch a queued Condition, if none exists attempt to fetch the Task
func (n *NatsHttpController) fetchCondition(ctx context.Context, serverID uuid.UUID) (*condition.Condition, error) {
	// if its a 404 || bad request just return
	errFetchCondition := errors.New("error fetching condition from queue")

	resp, err := n.orcQueryor.ConditionQueuePop(ctx, n.conditionKind, serverID)
	if err != nil {
		return nil, errors.Wrap(errFetchCondition, err.Error())
	}

	if resp == nil {
		return nil, errors.Wrap(errFetchCondition, "unexpected empty response")
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return n.conditionFromResponse(resp)
	case http.StatusNotFound:
		return nil, errors.Wrap(ErrNoCondition, "404, no task found")
	case http.StatusInternalServerError, http.StatusServiceUnavailable:
		return nil, errors.Wrap(ErrRetryRequest, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	case http.StatusBadRequest:
		return nil, errors.Wrap(ErrRetryRequest, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	default:
		return nil, errors.Wrap(errFetchCondition, fmt.Sprintf("unexpected status code %d, message: %s", resp.StatusCode, resp.Message))
	}
}

func (n *NatsHttpController) conditionFromResponse(resp *types.ServerResponse) (*condition.Condition, error) {
	errNoCondition := errors.New("no Condition object in response")

	if resp.Condition == nil {
		return nil, errNoCondition
	}

	return resp.Condition, nil
}

// TODO: reuse the ConditionTaskRepository interface for this
func (n *NatsHttpController) fetchTask(ctx context.Context, serverID uuid.UUID) (*condition.Task[any, any], error) {
	errFetchTask := errors.New("error fetching Condition Task from queue")

	resp, err := n.orcQueryor.ConditionTaskQuery(ctx, n.conditionKind, serverID)
	if err != nil {
		return nil, errors.Wrap(errFetchTask, err.Error())
	}

	if resp == nil {
		return nil, errors.Wrap(errFetchTask, "unexpected empty response")
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return n.taskFromResponse(resp)
	case http.StatusNotFound:
		return nil, errors.Wrap(errFetchTask, "404, no task found")
	case http.StatusInternalServerError, http.StatusServiceUnavailable:
		return nil, errors.Wrap(ErrRetryRequest, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	case http.StatusBadRequest:
		return nil, errors.Wrap(errFetchTask, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	default:
		return nil, errors.Wrap(errFetchTask, fmt.Sprintf("unexpected status code %d, message: %s", resp.StatusCode, resp.Message))
	}
}

func (n *NatsHttpController) taskFromResponse(resp *types.ServerResponse) (*condition.Task[any, any], error) {
	errNoTask := errors.New("no Task object in response")

	if resp.Task == nil {
		return nil, errNoTask
	}

	return resp.Task, nil
}
