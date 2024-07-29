package ctrl

import (
	"context"

	"github.com/metal-toolbox/rivets/condition"
)

// TaskHandler is passed in by the caller to be invoked when a message from the Jetstream is received for processing.
type TaskHandler interface {
	HandleTask(ctx context.Context, task *condition.Task[any, any], statusPublisher Publisher) error
}

type ConditionHandlerFactory func() TaskHandler
