package opinionatedsagas

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/markusylisiurunen/go-opinionatedevents"
)

type Step struct {
	HandleFunc     any
	CompensateFunc any
}

func (s *Step) isValid() error {
	// TODO: implement this
	return nil
}

func (s *Step) mountHandleFunc(
	receiver *opinionatedevents.Receiver,
	publisher *opinionatedevents.Publisher,
	queue string,
) error {
	name := reflect.New(reflect.ValueOf(s.HandleFunc).Type().In(1).Elem()).Interface().(task).TaskName()
	handle := func(ctx context.Context, _ string, delivery opinionatedevents.Delivery) opinionatedevents.ResultContainer {
		handleFuncValue := reflect.ValueOf(s.HandleFunc)
		handleFuncTaskType := handleFuncValue.Type().In(1)
		// initialize an empty task (based on the handle func's parameter type)
		taskMessage := newTaskMessage(reflect.New(handleFuncTaskType.Elem()).Interface().(task))
		// attempt to parse the payload
		if err := delivery.GetMessage().Payload(taskMessage); err != nil {
			// TODO: what to do if the message could not be parsed?
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		// execute the handle func with the task
		outValue := reflect.ValueOf(s.HandleFunc).Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(taskMessage.Task),
		})
		resultValue, compensateTaskValue, nextTaskValue := outValue[0], outValue[1], outValue[2]
		// check the result, if it's not success, return it right away
		result := resultValue.Interface().(opinionatedevents.ResultContainer)
		if result.GetResult().Err != nil {
			// TODO: move this rollback logic to its own middleware
			if delivery.GetAttempt() > 0 {
				rollbackMessage, ok := taskMessage.rollback()
				if !ok {
					return opinionatedevents.FatalResult(errors.New("failed too many times"))
				}
				rollbackOpinionatedMessage, err := rollbackMessage.toOpinionatedMessage()
				if err != nil {
					// FIXME: this entire function must be somehow atomic
					return opinionatedevents.ErrorResult(err, 30*time.Second)
				}
				if err := publisher.Publish(ctx, rollbackOpinionatedMessage); err != nil {
					// FIXME: this entire function must be somehow atomic
					return opinionatedevents.ErrorResult(err, 30*time.Second)
				}
				return opinionatedevents.FatalResult(errors.New("failed too many times"))
			}
			return result
		}
		// if there is no next task to be published, just return the result
		if nextTaskValue.IsNil() {
			return result
		}
		// construct and publish the next task
		rollbackHistory := taskMessage.RollbackStack.copy()
		if !compensateTaskValue.IsNil() {
			compensateTaskMessage := newTaskMessage(compensateTaskValue.Interface().(task))
			rollbackHistory.push(compensateTaskMessage.asRollbackStackItem())
		}
		nextTaskMessage := newTaskMessageWithRollbackHistory(
			nextTaskValue.Interface().(task),
			rollbackHistory,
		)
		nextTaskOpinionatedMessage, err := nextTaskMessage.toOpinionatedMessage()
		if err != nil {
			// FIXME: this entire function must be somehow atomic
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		if err := publisher.Publish(ctx, nextTaskOpinionatedMessage); err != nil {
			// FIXME: this entire function must be somehow atomic
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		// return the result
		return result
	}
	return receiver.On(queue, name,
		opinionatedevents.WithBackoff(opinionatedevents.LinearBackoff(10, 30, 5*time.Minute))(
			// TODO: should use `withRollback`
			handle,
		),
	)
}

func (s *Step) mountCompensateFunc(
	receiver *opinionatedevents.Receiver,
	publisher *opinionatedevents.Publisher,
	queue string,
) error {
	if s.CompensateFunc == nil {
		return nil
	}
	name := reflect.New(reflect.ValueOf(s.CompensateFunc).Type().In(1).Elem()).Interface().(task).TaskName()
	handle := func(ctx context.Context, _ string, delivery opinionatedevents.Delivery) opinionatedevents.ResultContainer {
		compensateFuncValue := reflect.ValueOf(s.CompensateFunc)
		compensateFuncTaskType := compensateFuncValue.Type().In(1)
		// initialize an empty task (based on the handle func's parameter type)
		taskMessage := newTaskMessage(reflect.New(compensateFuncTaskType.Elem()).Interface().(task))
		// attempt to parse the payload
		if err := delivery.GetMessage().Payload(taskMessage); err != nil {
			// TODO: what to do if the message could not be parsed?
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		// execute the handle func with the task
		outValue := reflect.ValueOf(s.CompensateFunc).Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(taskMessage.Task),
		})
		resultValue := outValue[0]
		// check the result, if it's not success, return it right away
		result := resultValue.Interface().(opinionatedevents.ResultContainer)
		if result.GetResult().Err != nil {
			return result
		}
		// success, publish the next rollback message from the stack
		rollbackMessage, ok := taskMessage.rollback()
		if !ok {
			return result
		}
		rollbackOpinionatedMessage, err := rollbackMessage.toOpinionatedMessage()
		if err != nil {
			// FIXME: this entire function must be somehow atomic
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		if err := publisher.Publish(ctx, rollbackOpinionatedMessage); err != nil {
			// FIXME: this entire function must be somehow atomic
			return opinionatedevents.ErrorResult(err, 30*time.Second)
		}
		// return the result
		return result
	}
	return receiver.On(queue, name,
		opinionatedevents.WithBackoff(opinionatedevents.LinearBackoff(10, 30, 5*time.Minute))(
			handle,
		),
	)
}
