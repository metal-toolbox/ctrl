package controller

import (
	"context"
	"sync"
	"time"

	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/pkg/kv"
	"github.com/metal-toolbox/rivets/events/registry"
	"github.com/sirupsen/logrus"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

var (
	once sync.Once

	checkinLivenessTTL = 3 * time.Minute
)

type LivenessCheckin interface {
	StartLivenessCheckin(ctx context.Context)
	ControllerID() registry.ControllerID
}

// NatsLiveness provides methods to register and periodically check into the controller registry.
//
// It implements the LivenessCheckin interface
type NatsLiveness struct {
	logger       *logrus.Logger
	stream       events.Stream
	natsConfig   events.NatsOptions
	controllerID registry.ControllerID
	interval     time.Duration
	hostname     string
}

// NewNatsLiveness returns a NATS implementation of the LivenessCheckin interface
func NewNatsLiveness(
	cfg events.NatsOptions, // nolint:gocritic // heavy param is heavy
	stream events.Stream,
	l *logrus.Logger,
	hostname string,
	interval time.Duration,
) LivenessCheckin {
	return &NatsLiveness{
		logger:     l,
		stream:     stream,
		natsConfig: cfg,
		hostname:   hostname,
		interval:   interval,
	}
}

func (n *NatsLiveness) checkinKVOpts() []kv.Option {
	opts := []kv.Option{
		kv.WithTTL(checkinLivenessTTL),
		kv.WithReplicas(n.natsConfig.KVReplicationFactor),
	}

	return opts
}

// Returns the controller ID for this instance
func (n *NatsLiveness) ControllerID() registry.ControllerID {
	return n.controllerID
}

// This starts a go-routine to peridically check in with the NATS kv
func (n *NatsLiveness) StartLivenessCheckin(ctx context.Context) {
	once.Do(func() {
		n.controllerID = registry.GetID(n.hostname)

		if err := registry.InitializeRegistryWithOptions(n.stream.(*events.NatsJetstream), n.checkinKVOpts()...); err != nil {
			metricsNATSError("initialize liveness registry")
			n.logger.WithError(err).Error("unable to initialize active controller registry")
			return
		}

		go n.checkinRoutine(ctx)
	})
}

func (n *NatsLiveness) checkinRoutine(ctx context.Context) {
	if err := registry.RegisterController(n.controllerID); err != nil {
		n.logger.WithError(err).Warn("unable to do initial controller liveness registration")
	}

	tick := time.NewTicker(n.interval)
	defer tick.Stop()

	var stop bool
	for !stop {
		select {
		case <-tick.C:
			err := registry.ControllerCheckin(n.controllerID)
			if err != nil {
				n.logger.WithError(err).
					WithField("id", n.controllerID.String()).
					Warn("controller check-in failed")
				metricsNATSError("liveness check-in")
				if err = refreshControllerToken(n.controllerID); err != nil {
					n.logger.WithError(err).
						WithField("id", n.controllerID.String()).
						Fatal("unable to refresh controller liveness token")
				}
			}
		case <-ctx.Done():
			n.logger.Info("liveness check-in stopping on done context")
			stop = true
		}
	}
}

// try to de-register/re-register this id.
func refreshControllerToken(id registry.ControllerID) error {
	err := registry.DeregisterController(id)
	if err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
		metricsNATSError("liveness refresh: de-register")
		return err
	}
	err = registry.RegisterController(id)
	if err != nil {
		metricsNATSError("liveness refresh: register")
		return err
	}
	return nil
}
