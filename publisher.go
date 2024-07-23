package controller

import (
	"context"

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
	Publish(ctx context.Context, task *condition.Task[any, any], tsUpdate bool) error
}

type PublisherHTTP struct {
	logger               *logrus.Logger
	statusValuePublisher *HTTPConditionStatusPublisher
	taskRepository       *HTTPTaskRepository
}

func NewHTTPPublisher(serverID,
	conditionID uuid.UUID,
	conditionKind condition.Kind,
	controllerID registry.ControllerID,
	orcQueryor orc.Queryor,
	logger *logrus.Logger) Publisher {
	p := &PublisherHTTP{logger: logger}
	httpStatusValuePublisher := NewHTTPConditionStatusPublisher(
		serverID,
		conditionID,
		conditionKind,
		controllerID,
		orcQueryor,
		logger,
	)

	p.statusValuePublisher = httpStatusValuePublisher.(*HTTPConditionStatusPublisher)

	httpTaskRepository := NewHTTPTaskRepository(
		serverID,
		conditionID,
		conditionKind,
		orcQueryor,
		logger,
	)

	p.taskRepository = httpTaskRepository.(*HTTPTaskRepository)

	return p
}

func (p *PublisherHTTP) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdate bool) error {
	err := p.statusValuePublisher.Publish(ctx, task.Server.ID, task.State, task.Status.MustMarshal(), tsUpdate)
	if err != nil {
		p.logger.WithError(err).Error("Status Value publish error")
		return err
	}

	err = p.taskRepository.Publish(ctx, task, tsUpdate)
	if err != nil {
		p.logger.WithError(err).Error("Task publish error")
		return err
	}

	return nil
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

func (p *PublisherNATS) Publish(ctx context.Context, task *condition.Task[any, any], tsUpdate bool) error {
	err := p.statusValuePublisher.Publish(ctx, task.Server.ID, task.State, task.Status.MustMarshal(), tsUpdate)
	if err != nil {
		p.logger.WithError(err).Error("Status Value publish error")
		return err
	}

	err = p.taskRepository.Publish(ctx, task, tsUpdate)
	if err != nil {
		p.logger.WithError(err).Error("Task publish error")
		return err
	}

	return nil
}
