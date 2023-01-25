package opinionatedsagas

import (
	"context"

	"github.com/markusylisiurunen/go-opinionatedevents"
)

type Saga struct {
	receiver  *opinionatedevents.Receiver
	publisher *opinionatedevents.Publisher
	queue     string

	steps []*Step
}

func NewSaga(receiver *opinionatedevents.Receiver, publisher *opinionatedevents.Publisher, queue string) *Saga {
	return &Saga{receiver: receiver, publisher: publisher, queue: queue, steps: []*Step{}}
}

func (s *Saga) AddStep(step *Step) {
	s.steps = append(s.steps, step)
}

func (s *Saga) RegisterHandlers() error {
	if len(s.steps) == 0 {
		return nil
	}
	for _, step := range s.steps {
		if err := step.isValid(); err != nil {
			return err
		}
	}
	for _, step := range s.steps {
		if err := step.mountHandleFunc(s.receiver, s.publisher, s.queue); err != nil {
			return err
		}
		if err := step.mountCompensateFunc(s.receiver, s.publisher, s.queue); err != nil {
			return err
		}
	}
	return nil
}

func (s *Saga) Trigger(ctx context.Context, task task) error {
	// TODO: validate that the task is of the same type as the first step's handle func
	msg, err := newTaskMessage(task).toOpinionatedMessage()
	if err != nil {
		return err
	}
	return s.publisher.Publish(ctx, msg)
}
