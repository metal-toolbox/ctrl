package ctrl

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/coreos/go-oidc"
	"github.com/metal-toolbox/rivets/condition"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	pkgHTTPController = "events/httpcontroller"
	// max number of times times to retry the Orc API queries
	// 300 * 30s = 2.5h
	orcQueryRetries = 300

	// query interval duration
	queryInterval = 30 * time.Second

	// Orchestrator API query timeout
	orcQueryTimeout = 60 * time.Second
)

var (
	ErrHandlerInit    = errors.New("error initializing handler")
	ErrEmptyResponse  = errors.New("empty response with error")
	errRetryRequest   = errors.New("request retry required")
	ErrNoCondition    = errors.New("no condition available")
	errNothingToDo    = errors.New("nothing to do here")
	errFetchTask      = errors.New("error fetching Task object")
	errFetchCondition = errors.New("error fetching Condition object")
)

// HTTPController implements the TaskHandler interface to interact with the NATS queue, KV over HTTP(s)
type HTTPController struct {
	appName         string
	logger          *logrus.Logger
	facilityCode    string
	serverID        uuid.UUID
	conditionKind   condition.Kind
	orcQueryRetries int
	queryInterval   time.Duration
	handlerTimeout  time.Duration
	orcQueryor      orc.Queryor
}

type OrchestratorAPIConfig struct {
	AuthDisabled         bool
	Endpoint             string
	AuthToken            string
	OidcIssuerEndpoint   string
	OidcAudienceEndpoint string
	OidcClientSecret     string
	OidcClientID         string
	OidcClientScopes     []string
}

// OptionHTTPController sets parameters on the HTTPController
type OptionHTTPController func(*HTTPController)

func NewHTTPController(
	appName,
	facilityCode string,
	serverID uuid.UUID,
	conditionKind condition.Kind,
	orcClientCfg *OrchestratorAPIConfig,
	options ...OptionHTTPController) (*HTTPController, error) {

	logger := logrus.New()
	logger.Formatter = &logrus.JSONFormatter{}

	nhc := &HTTPController{
		appName:         appName,
		facilityCode:    facilityCode,
		serverID:        serverID,
		conditionKind:   conditionKind,
		orcQueryRetries: orcQueryRetries,
		handlerTimeout:  handlerTimeout,
		queryInterval:   queryInterval,
		logger:          logger,
	}

	for _, opt := range options {
		opt(nhc)
	}

	if nhc.orcQueryor == nil {
		orcQueryor, err := newConditionsAPIClient(orcClientCfg)
		if err != nil {
			return nil, errors.Wrap(ErrHandlerInit, "error in Conditions API client init: "+err.Error())
		}

		nhc.orcQueryor = orcQueryor
	}

	return nhc, nil
}

func newConditionsAPIClient(cfg *OrchestratorAPIConfig) (orc.Queryor, error) {
	if cfg.AuthDisabled {
		client := http.DefaultClient
		client.Timeout = orcQueryTimeout
		return orc.NewClient(
			cfg.Endpoint,
			orc.WithHTTPClient(client),
		)
	}

	client, err := newOAuthClient(cfg)
	if err != nil {
		return nil, err
	}

	return orc.NewClient(
		cfg.Endpoint,
		orc.WithHTTPClient(client),
		orc.WithAuthToken(cfg.AuthToken),
	)
}

// returns an http client setup with oauth and otelhttp
func newOAuthClient(cfg *OrchestratorAPIConfig) (*http.Client, error) {
	errProvider := errors.New("orchestrator client OIDC provider setup error")

	// otel http client
	client := otelhttp.DefaultClient
	client.Timeout = orcQueryTimeout

	// context for OIDC issuer endpoint query
	ctxp, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// setup oidc provider
	provider, err := oidc.NewProvider(ctxp, cfg.OidcIssuerEndpoint)
	if err != nil {
		return nil, errors.Wrap(errProvider, err.Error())
	}

	// setup oauth configuration
	oauthConfig := clientcredentials.Config{
		ClientID:       cfg.OidcClientID,
		ClientSecret:   cfg.OidcClientSecret,
		TokenURL:       provider.Endpoint().TokenURL,
		Scopes:         cfg.OidcClientScopes,
		EndpointParams: url.Values{"audience": []string{cfg.OidcAudienceEndpoint}},
	}

	oAuthclient := oauthConfig.Client(context.Background())
	client.Transport = oAuthclient.Transport
	client.Jar = oAuthclient.Jar

	return client, nil
}

// Sets a logger on the controller
func WithNATSHTTPLogger(logger *logrus.Logger) OptionHTTPController {
	return func(n *HTTPController) {
		n.logger = logger
	}
}

// Sets the Orchestrator API queryor client
func WithOrchestratorClient(c orc.Queryor) OptionHTTPController {
	return func(n *HTTPController) {
		n.orcQueryor = c
	}
}

func (n *HTTPController) traceSpaceContextFromValues(traceID, spanID string) (trace.SpanContext, error) {
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

func (n *HTTPController) Run(ctx context.Context, handler TaskHandler) error {
	ctx, span := otel.Tracer(pkgHTTPController).Start(
		ctx,
		"Run",
	)
	defer span.End()

	var err error

	task, err := n.fetchTaskWithRetries(ctx, n.serverID, n.orcQueryRetries, n.queryInterval)
	if err != nil {
		return errors.Wrap(ErrHandlerInit, err.Error())
	}

	// init publisher
	publisher := NewHTTPPublisher(n.appName, n.serverID, task.ID, n.conditionKind, n.orcQueryor, n.logger)
	if task.State == condition.Pending {
		task.Status.Append("In process by controller: " + n.serverID.String())
	} else {
		task.Status.Append("resumed by controller: " + n.serverID.String())
	}

	if errPublish := publisher.Publish(ctx, task, false); errPublish != nil {
		msg := "error publishing initial Task, Status KV record, condition aborted"
		n.logger.WithError(errPublish).WithFields(logrus.Fields{
			"conditionID": task.ID.String(),
		}).Error(msg)

		return errors.Wrap(errPublish, msg)
	}

	// set remote span context
	remoteSpanCtx, err := n.traceSpaceContextFromValues(task.TraceID, task.SpanID)
	if err != nil {
		n.logger.Debug(err.Error())
	} else {
		// overwrite span context with remote span when available
		var span trace.Span
		ctx, span = otel.Tracer(pkgHTTPController).Start(
			trace.ContextWithRemoteSpanContext(ctx, remoteSpanCtx),
			"Run",
		)
		defer span.End()
	}

	return n.runTaskWithMonitor(ctx, handler, task, publisher, statusInterval)
}

func (n *HTTPController) runTaskWithMonitor(
	ctx context.Context,
	handler TaskHandler,
	task *condition.Task[any, any],
	publisher Publisher,
	publishInterval time.Duration,
) error {
	ctx, span := otel.Tracer(pkgHTTPController).Start(
		ctx,
		"runTaskWithMonitor",
	)
	defer span.End()
	// doneCh indicates the handler run completed
	doneCh := make(chan bool)

	// monitor updates TS on status until the task handler returns.
	monitor := func() {
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()

		// periodically update the LastUpdate TS in status KV,
		/// which keeps the Orchestrator from reconciling this condition.
	Loop:
		for {
			select {
			case <-ticker.C:
				if errPublish := publisher.Publish(
					ctx,
					task,
					true,
				); errPublish != nil {
					n.logger.WithError(errPublish).Error("failed to publish update")
				}

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

	logger.Info("Controller initialized, running task..")

	// set handler timeout
	handlerCtx, cancel := context.WithTimeout(ctx, n.handlerTimeout)
	defer cancel()

	if err := handler.HandleTask(handlerCtx, task, publisher); err != nil {
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

func (n *HTTPController) fetchTaskWithRetries(ctx context.Context, serverID uuid.UUID, tries int, interval time.Duration) (*condition.Task[any, any], error) {
	for attempt := 0; attempt <= tries; attempt++ {
		le := n.logger.WithField("attempt", fmt.Sprintf("%d/%d", attempt, tries))

		if attempt > 0 {
			// returns error on context cancellation
			if errSleep := sleepWithContext(ctx, interval); errSleep != nil {
				return nil, errSleep
			}
		}

		// fetch condition
		le.Info("Fetching Condition..")
		cond, err := n.fetchCondition(ctx, serverID)
		if err != nil {
			le.WithError(err).Warn("Condition fetch error")
			if errors.Is(err, errRetryRequest) {
				continue
			}

			return nil, err
		}

		// kind matches configured
		if cond.Kind != n.conditionKind {
			le.WithFields(logrus.Fields{
				"conditionID": cond.ID,
				"received":    cond.Kind,
				"expect":      n.conditionKind,
				"state":       cond.State,
			}).Debug("waiting for configured condition kind...")

			continue
		}

		// state finalized
		if condition.StateIsComplete(cond.State) {
			le.WithFields(logrus.Fields{
				"conditionID": cond.ID,
				"received":    cond.Kind,
				"expect":      n.conditionKind,
				"state":       cond.State,
			}).Info("condition state is final, nothing to do here.")

			return nil, errNothingToDo
		}

		// state pending
		if cond.State == condition.Pending {
			return condition.NewTaskFromCondition(cond), nil
		}

		// state active
		if cond.State == condition.Active {
			task, errFetch := n.fetchTask(ctx, serverID)
			if errFetch != nil {
				le.WithError(errFetch).Warn("Task fetch error")
				if errors.Is(errFetch, errRetryRequest) {
					continue
				}

				return nil, errFetch
			}

			return task, nil
		}
	}

	return nil, errNothingToDo
}

// fetch condition - this will retrieve the current active/pending condition
func (n *HTTPController) fetchCondition(ctx context.Context, serverID uuid.UUID) (*condition.Condition, error) {
	resp, err := n.orcQueryor.ConditionQuery(ctx, serverID)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil, errors.Wrap(errRetryRequest, "unexpected empty response")
		}

		return nil, errors.Wrap(errFetchCondition, err.Error())
	}

	if resp == nil {
		return nil, errors.Wrap(errRetryRequest, "unexpected empty response")
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if resp.Condition == nil {
			return nil, errors.Wrap(errFetchCondition, "got nil object")
		}
		return resp.Condition, nil
	case http.StatusNotFound, http.StatusInternalServerError:
		return nil, errors.Wrap(errRetryRequest, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	case http.StatusBadRequest:
		return nil, errors.Wrap(errFetchCondition, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	default:
		return nil, errors.Wrap(errFetchCondition, fmt.Sprintf("unexpected status code %d, message: %s", resp.StatusCode, resp.Message))
	}
}

func (n *HTTPController) fetchTask(ctx context.Context, serverID uuid.UUID) (*condition.Task[any, any], error) {
	resp, err := n.orcQueryor.ConditionTaskQuery(ctx, n.conditionKind, serverID)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil, errors.Wrap(errRetryRequest, "unexpected empty response")
		}

		return nil, errors.Wrap(errFetchTask, err.Error())
	}

	if resp == nil {
		return nil, errors.Wrap(errRetryRequest, "unexpected empty response")
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if resp.Task == nil {
			return nil, errors.Wrap(errFetchTask, "got nil object")
		}
		return resp.Task, nil
	case http.StatusNotFound, http.StatusInternalServerError, http.StatusUnprocessableEntity:
		return nil, errors.Wrap(errRetryRequest, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	case http.StatusBadRequest:
		return nil, errors.Wrap(errFetchTask, fmt.Sprintf("%d, message: %s", resp.StatusCode, resp.Message))
	default:
		return nil, errors.Wrap(errFetchTask, fmt.Sprintf("unexpected status code %d, message: %s", resp.StatusCode, resp.Message))
	}
}
