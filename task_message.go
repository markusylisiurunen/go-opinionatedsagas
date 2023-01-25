package opinionatedsagas

import (
	"encoding/json"
	"errors"

	"github.com/markusylisiurunen/go-opinionatedevents"
)

type task interface {
	TaskName() string
}

type anyTask struct {
	name string
	task map[string]any
}

func (t *anyTask) TaskName() string {
	return t.name
}

func (t *anyTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.task)
}

func (t *anyTask) UnmarshalJSON(data []byte) error {
	return errors.New("unexpectedly invoked anyTask's UnmarshalJSON method")
}

type taskMessage struct {
	Meta          map[string]any `json:"meta"`
	RollbackStack *rollbackStack `json:"rollback_stack"`
	Task          task           `json:"task"`
}

func newTaskMessage(task task) *taskMessage {
	return &taskMessage{
		Meta:          map[string]any{},
		RollbackStack: newRollbackStack(),
		Task:          task,
	}
}

func newTaskMessageWithRollbackHistory(nextTask task, rollbackHistory *rollbackStack) *taskMessage {
	return &taskMessage{
		Meta:          map[string]any{},
		RollbackStack: rollbackHistory,
		Task:          nextTask,
	}
}

func (t *taskMessage) TaskName() string {
	return t.Task.TaskName()
}

func (t *taskMessage) rollback() (*taskMessage, bool) {
	rollbackHistory := t.RollbackStack.copy()
	compensate, ok := rollbackHistory.pop()
	if !ok {
		return nil, false
	}
	return &taskMessage{
		Meta:          compensate.Meta,
		RollbackStack: rollbackHistory,
		Task:          &anyTask{compensate.Name, compensate.Task},
	}, true
}

func (t *taskMessage) asRollbackStackItem() *rollbackStackItem {
	task := map[string]any{}
	data, err := json.Marshal(t.Task)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &task); err != nil {
		panic(err)
	}
	return &rollbackStackItem{Name: t.TaskName(), Meta: t.Meta, Task: task}
}

// TODO: remove these once go-opinionatedevents won't require them anymore
func (t *taskMessage) MarshalPayload() ([]byte, error)    { return json.Marshal(t) }
func (t *taskMessage) UnmarshalPayload(data []byte) error { return json.Unmarshal(data, t) }

func (t *taskMessage) toOpinionatedMessage() (*opinionatedevents.Message, error) {
	return opinionatedevents.NewMessage(t.TaskName(), t)
}
