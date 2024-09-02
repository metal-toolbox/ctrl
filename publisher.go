package ctrl

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/metal-toolbox/rivets/condition"
	"github.com/metal-toolbox/rivets/events"
	"github.com/metal-toolbox/rivets/events/registry"
	"github.com/sirupsen/logrus"

	orc "github.com/metal-toolbox/conditionorc/pkg/api/v1/orchestrator/client"
)

// The Publisher interface wraps the Task and StatusValue publishers into one,
// such that the caller invokes Publish and this interface takes care of publishing the status and the Task.
//
// Subsequently the Task updates is all that is to be published, replacing the statusValue updates.
type Publisher interface {
	Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error
}

type PublisherHTTP struct {
	logger               *logrus.Logger
	statusValuePublisher *HTTPConditionStatusPublisher
	taskRepository       *HTTPTaskRepository
}

func NewHTTPPublisher(
	appName string,
	serverID,
	conditionID uuid.UUID,
	conditionKind condition.Kind,
	orcQueryor orc.Queryor,
	logger *logrus.Logger) Publisher {
	p := &PublisherHTTP{logger: logger}
	httpStatusValuePublisher := NewHTTPConditionStatusPublisher(
		appName,
		serverID,
		conditionID,
		conditionKind,
		orcQueryor,
		logger,
	)

	p.statusValuePublisher = httpStatusValuePublisher.(*HTTPConditionStatusPublisher)

	httpTaskRepository := NewHTTPTaskRepository(
		appName,
		serverID,
		conditionID,
		conditionKind,
		orcQueryor,
		logger,
	)

	p.taskRepository = httpTaskRepository.(*HTTPTaskRepository)

	return p
}

func (p *PublisherHTTP) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error {
	var err error

	if errTask := p.taskRepository.Publish(ctx, task, tsUpdateOnly); errTask != nil {
		p.logger.WithError(errTask).Error("Task publish error")
		err = errors.Join(err, errTask)
	}

	errSV := p.statusValuePublisher.Publish(ctx, task.Server.ID, task.State, task.Status.MustMarshal(), tsUpdateOnly)
	if errSV != nil {
		p.logger.WithError(errSV).Error("Status Value publish error")
		err = errors.Join(err, errSV)
	}

	return err
}

type PublisherNATS struct {
	logger               *logrus.Logger
	statusValuePublisher *NatsConditionStatusPublisher
	taskRepository       *NatsConditionTaskRepository
}

func NewNatsPublisher(
	appName,
	conditionID,
	serverID,
	facilityCode string,
	conditionKind condition.Kind,
	controllerID registry.ControllerID,
	kvReplicas int,
	stream *events.NatsJetstream,
	logger *logrus.Logger,
) (Publisher, error) {
	p := &PublisherNATS{logger: logger}
	svPublisher, err := NewNatsConditionStatusPublisher(
		appName,
		conditionID,
		facilityCode,
		conditionKind,
		controllerID,
		kvReplicas,
		stream,
		logger,
	)
	if err != nil {
		return nil, err
	}

	p.statusValuePublisher = svPublisher.(*NatsConditionStatusPublisher)

	taskRepository, err := NewNatsConditionTaskRepository(
		conditionID,
		serverID,
		facilityCode,
		conditionKind,
		controllerID,
		kvReplicas,
		stream,
		logger,
	)
	if err != nil {
		return nil, err
	}

	p.taskRepository = taskRepository.(*NatsConditionTaskRepository)

	return p, nil
}

func (p *PublisherNATS) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdateOnly bool) error {
	var err error

	errTask := p.taskRepository.Publish(ctx, task, tsUpdateOnly)
	if errTask != nil {
		p.logger.WithError(errTask).Error("Task publish error")
		err = errors.Join(err, errTask)
	}

	errSV := p.statusValuePublisher.Publish(ctx, task.Server.ID, task.State, task.Status.MustMarshal(), tsUpdateOnly)
	if errSV != nil {
		p.logger.WithError(errSV).Error("Status Value publish error")
		err = errors.Join(err, errSV)
	}

	return err
}
