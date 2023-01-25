package opinionatedsagas

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskMessage(t *testing.T) {
	t.Run("marshals and unmarshals a task message", func(t *testing.T) {
		compensate := newTaskMessage(&testTask{"compensate_1"})
		rollbackHistory := newRollbackStack()
		rollbackHistory.push(compensate.asRollbackStackItem())
		task := newTaskMessageWithRollbackHistory(&testTask{"hello world"}, rollbackHistory)
		data, err := json.Marshal(task)
		assert.NoError(t, err)
		task = newTaskMessage(&testTask{})
		assert.NoError(t, json.Unmarshal(data, task))
		assert.Equal(t, 0, len(task.Meta))
		assert.Equal(t, 1, len(task.RollbackStack.stack))
		assert.Equal(t, "tasks.test", task.TaskName())
		assert.Equal(t, "hello world", task.Task.(*testTask).Value)
	})

	t.Run("handles rolling back correctly", func(t *testing.T) {
		// construct the rollback history
		compensateOne := newTaskMessage(&testTask{"compensate_1"})
		compensateTwo := newTaskMessage(&testTask{"compensate_2"})
		rollbackHistory := newRollbackStack()
		rollbackHistory.push(compensateOne.asRollbackStackItem())
		rollbackHistory.push(compensateTwo.asRollbackStackItem())
		// build the "failing" task itself
		task := newTaskMessageWithRollbackHistory(&testTask{"task_1"}, rollbackHistory)
		// fake a rollback call and assert the result
		rollbackTo, ok := task.rollback()
		assert.True(t, ok)
		assert.Equal(t, "tasks.test", rollbackTo.TaskName())
		assert.Equal(t, "compensate_2", rollbackTo.Task.(*anyTask).task["value"])
		// assert the remaining rollback stack by rolling back again
		nextRollbackTo, ok := rollbackTo.rollback()
		assert.True(t, ok)
		assert.Equal(t, "tasks.test", nextRollbackTo.TaskName())
		assert.Equal(t, "compensate_1", nextRollbackTo.Task.(*anyTask).task["value"])
	})

	t.Run("survives serialization at every step of a rollback sequence", func(t *testing.T) {
		serializeAndDeserialize := func(src *taskMessage) *taskMessage {
			data, err := json.Marshal(src)
			assert.NoError(t, err)
			dest := newTaskMessage(&testTask{})
			assert.NoError(t, json.Unmarshal(data, dest))
			return dest
		}
		// construct the rollback history
		compensateOne := newTaskMessage(&testTask{"compensate_1"})
		compensateTwo := newTaskMessage(&testTask{"compensate_2"})
		rollbackHistory := newRollbackStack()
		rollbackHistory.push(compensateOne.asRollbackStackItem())
		rollbackHistory.push(compensateTwo.asRollbackStackItem())
		// construct the initial task message with the rollback history
		task := newTaskMessageWithRollbackHistory(&testTask{"original_task"}, rollbackHistory)
		task = serializeAndDeserialize(task)
		// first rollback
		task, ok := task.rollback()
		assert.True(t, ok)
		task = serializeAndDeserialize(task)
		assert.Equal(t, "tasks.test", task.TaskName())
		assert.Equal(t, "compensate_2", task.Task.(*testTask).Value)
		task = serializeAndDeserialize(task)
		// second rollback
		task, ok = task.rollback()
		assert.True(t, ok)
		task = serializeAndDeserialize(task)
		assert.Equal(t, "tasks.test", task.TaskName())
		assert.Equal(t, "compensate_1", task.Task.(*testTask).Value)
		task = serializeAndDeserialize(task)
		// third rollback (there's nothing to roll back to)
		_, ok = task.rollback()
		assert.False(t, ok)
	})
}

type testTask struct {
	Value string `json:"value"`
}

func (t *testTask) TaskName() string {
	return "tasks.test"
}
