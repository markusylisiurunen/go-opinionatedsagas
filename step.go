package opinionatedsagas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"
)

type Step struct {
	db     *sql.DB
	schema string

	publisher *events.Publisher
	receiver  *events.Receiver
	queue     string

	HandleFunc     any
	CompensateFunc any

	MaxAttempts int
}

func (s *Step) init(db *sql.DB, schema string, publisher *events.Publisher, receiver *events.Receiver, queue string) {
	s.db = db
	s.schema = schema
	s.publisher = publisher
	s.receiver = receiver
	s.queue = queue
}

func (s *Step) getMaxAttempts() int {
	return s.MaxAttempts
}

func (s *Step) isForTask(task task) bool {
	handleFuncValue := reflect.ValueOf(s.HandleFunc)
	return handleFuncValue.Type().In(1) == reflect.TypeOf(task)
}

func (s *Step) isValid() error {
	if err := s.handleFuncIsValid(); err != nil {
		return err
	}
	if err := s.compensateFuncIsValid(); err != nil {
		return err
	}
	return nil
}

func (s *Step) handleFuncIsValid() error {
	if s.HandleFunc == nil {
		return errors.New("the handle func must be provided")
	}
	funcValue := reflect.ValueOf(s.HandleFunc)
	// get the count of function arguments and return values
	inCount := funcValue.Type().NumIn()
	outCount := funcValue.Type().NumOut()
	// validate all cases of 2 arguments
	if inCount == 2 {
		if !isInterface[context.Context](funcValue.Type().In(0)) {
			return errors.New("the first handle func argument must implement 'context.Context'")
		}
		if !isInterface[task](funcValue.Type().In(1)) {
			return errors.New("the second handle func argument must implement 'task'")
		}
		// validate the case of 2 -> 1
		if outCount == 1 {
			if !isInterface[error](funcValue.Type().Out(0)) {
				return errors.New("the first handle func return value must implement 'error'")
			}
			return nil
		}
		// validate the case of 2 -> 3
		if outCount == 3 {
			if !isInterface[task](funcValue.Type().Out(0)) {
				return errors.New("the first handle func return value must implement 'task'")
			}
			if !isInterface[task](funcValue.Type().Out(1)) {
				return errors.New("the second handle func return value must implement 'task'")
			}
			if !isInterface[error](funcValue.Type().Out(2)) {
				return errors.New("the third handle func return value must implement 'error'")
			}
			return nil
		}
	}
	return errors.New("an unknown combination of handle func arguments and return values")
}

func (s *Step) compensateFuncIsValid() error {
	if s.CompensateFunc == nil {
		return nil
	}
	funcValue := reflect.ValueOf(s.CompensateFunc)
	// get the count of function arguments and return values
	inCount := funcValue.Type().NumIn()
	outCount := funcValue.Type().NumOut()
	// validate the case of 2 -> 1
	if inCount == 2 && outCount == 1 {
		if !isInterface[context.Context](funcValue.Type().In(0)) {
			return errors.New("the first compensate func argument must implement 'context.Context'")
		}
		if !isInterface[task](funcValue.Type().In(1)) {
			return errors.New("the second compensate func argument must implement 'task'")
		}
		if !isInterface[error](funcValue.Type().Out(0)) {
			return errors.New("the first compensate func return value must implement 'error'")
		}
		return nil
	}
	return errors.New("an unknown combination of compensate func arguments and return values")
}

func (s *Step) mountHandleFunc() error {
	name := reflect.New(reflect.ValueOf(s.HandleFunc).Type().In(1).Elem()).Interface().(task).TaskName()
	handle := func(ctx context.Context, delivery events.Delivery) error {
		handleFuncValue := reflect.ValueOf(s.HandleFunc)
		handleFuncTaskType := handleFuncValue.Type().In(1)
		// initialize an empty task (based on the handle func's parameter type)
		taskMessage := newTaskMessage(reflect.New(handleFuncTaskType.Elem()).Interface().(task))
		// attempt to parse the payload
		if err := delivery.GetMessage().GetPayload(taskMessage); err != nil {
			// TODO: what to do if the message could not be parsed?
			return err
		}
		// execute the handle func with the task
		outValue := reflect.ValueOf(s.HandleFunc).Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(taskMessage.Task),
		})
		var (
			resultValue         reflect.Value
			compensateTaskValue reflect.Value
			nextTaskValue       reflect.Value
		)
		if len(outValue) == 1 {
			resultValue = outValue[0]
			// check the result, if it's not success, return it right away
			if !resultValue.IsNil() {
				return resultValue.Interface().(error)
			}
			return nil
		}
		if len(outValue) == 3 {
			compensateTaskValue = outValue[0]
			nextTaskValue = outValue[1]
			resultValue = outValue[2]
			// check the result, if it's not success, return it right away
			if !resultValue.IsNil() {
				return resultValue.Interface().(error)
			}
		}
		// if there is no next task to be published, just return the result
		if nextTaskValue.IsNil() {
			return nil
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
			return err
		}
		if err := s.publisher.Publish(ctx, nextTaskOpinionatedMessage); err != nil {
			// FIXME: this entire function must be somehow atomic
			return err
		}
		// return the result
		return nil
	}
	return s.receiver.On(s.queue, fmt.Sprintf("tasks.%s", name),
		middlewares{
			// 10s, 15s, 30s, 1m21s, 4m11s, 13m35s, 30m0s, 30m0s
			events.WithBackoff(events.ExponentialBackoff(10, 2, 1.2, 30*time.Minute)),
			withRollback(s.publisher, s.getMaxAttempts()),
			withIdempotent(s.db, s.schema),
		}.
			wrap(handle),
	)
}

func (s *Step) mountCompensateFunc() error {
	if s.CompensateFunc == nil {
		return nil
	}
	name := reflect.New(reflect.ValueOf(s.CompensateFunc).Type().In(1).Elem()).Interface().(task).TaskName()
	handle := func(ctx context.Context, delivery events.Delivery) error {
		compensateFuncValue := reflect.ValueOf(s.CompensateFunc)
		compensateFuncTaskType := compensateFuncValue.Type().In(1)
		// initialize an empty task (based on the handle func's parameter type)
		taskMessage := newTaskMessage(reflect.New(compensateFuncTaskType.Elem()).Interface().(task))
		// attempt to parse the payload
		if err := delivery.GetMessage().GetPayload(taskMessage); err != nil {
			// TODO: what to do if the message could not be parsed?
			return err
		}
		// execute the handle func with the task
		outValue := reflect.ValueOf(s.CompensateFunc).Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(taskMessage.Task),
		})
		resultValue := outValue[0]
		// check the result, if it's not success, return it right away
		if !resultValue.IsNil() {
			return resultValue.Interface().(error)
		}
		// success, publish the next rollback message from the stack
		rollbackMessage, ok := taskMessage.rollback()
		if !ok {
			return nil
		}
		rollbackOpinionatedMessage, err := rollbackMessage.toOpinionatedMessage()
		if err != nil {
			// FIXME: this entire function must be somehow atomic
			return err
		}
		if err := s.publisher.Publish(ctx, rollbackOpinionatedMessage); err != nil {
			// FIXME: this entire function must be somehow atomic
			return err
		}
		// return the result
		return nil
	}
	return s.receiver.On(s.queue, fmt.Sprintf("tasks.%s", name),
		middlewares{
			// 10s, 15s, 30s, 1m21s, 4m11s, 13m35s, 30m0s, 30m0s
			events.WithBackoff(events.ExponentialBackoff(10, 2, 1.2, 30*time.Minute)),
			withIdempotent(s.db, s.schema),
		}.
			wrap(handle),
	)
}
