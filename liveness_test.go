package controller

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/registry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestStartLivenessCheckin(t *testing.T) {
	// Ignore can be removed if this ever gets fixed,
	//
	// https://github.com/census-instrumentation/opencensus-go/issues/1191
	defer goleak.VerifyNone(t, []goleak.Option{goleak.IgnoreCurrent()}...)

	srv := startJetStreamServer(t)
	defer shutdownJetStream(t, srv)
	natsConn, _ := jetStreamContext(t, srv) // nc is closed on evJS.Close(), js needs no cleanup
	evJS := events.NewJetstreamFromConn(natsConn)
	defer evJS.Close()

	hostname, _ := os.Hostname()
	n := &NatsController{
		hostname:        hostname,
		logger:          logrus.New(),
		stream:          evJS,
		checkinInterval: checkinInterval,
	}

	n.liveness = NewNatsLiveness(n.natsConfig, n.stream, n.logger, n.hostname, checkinInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.liveness.StartLivenessCheckin(ctx)

	var ts time.Time
	var errLastContact error
	// wait for checkin routine to complete - 5 seconds should be enough?
	for i := 0; i <= 5; i++ {
		ts, errLastContact = registry.LastContact(n.liveness.ControllerID())
		if errLastContact == nil {
			break
		}

		time.Sleep(1 * time.Second)
	}

	assert.Nil(t, errLastContact)
	assert.NotZero(t, ts)
}
