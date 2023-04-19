package opinionatedsagas

import (
	"context"
	"encoding/base64"
	"fmt"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/tidwall/gjson"
)

// a test task
// ---

type testTask struct {
	name  string `json:"-"`
	Value string `json:"value"`
}

func newTestTask(name string, value string) *testTask {
	return &testTask{name, value}
}

func (t *testTask) TaskName() string {
	return t.name
}

// a simple in-memory publisher destination for testing
// ---

type testDestination struct {
	err      error
	messages [][]byte
}

func newTestDestination() *testDestination {
	return &testDestination{err: nil, messages: [][]byte{}}
}

func (d *testDestination) Deliver(ctx context.Context, msg *events.Message) error {
	if d.err != nil {
		return d.err
	}
	message, err := msg.MarshalJSON()
	if err != nil {
		return err
	}
	d.messages = append(d.messages, message)
	return nil
}

func (d *testDestination) getTaskName(idx int) string {
	if idx > len(d.messages)-1 {
		return ""
	}
	return gjson.Get(string(d.messages[idx]), "name").String()
}

func (d *testDestination) getRollbackStackSize(idx int) int {
	if idx > len(d.messages)-1 {
		return -1
	}
	payload, err := base64.StdEncoding.DecodeString(
		gjson.Get(string(d.messages[idx]), "payload").String(),
	)
	if err != nil {
		return -1
	}
	return len(gjson.Get(string(payload), "rollback_stack").Array())
}

func (d *testDestination) getRollbackStackTaskName(messageIdx int, stackIdx int) string {
	if messageIdx > len(d.messages)-1 {
		return ""
	}
	payload, err := base64.StdEncoding.DecodeString(
		gjson.Get(string(d.messages[messageIdx]), "payload").String(),
	)
	if err != nil {
		return ""
	}
	stack := gjson.Get(string(payload), "rollback_stack").Array()
	if stackIdx > len(stack)-1 {
		return ""
	}
	return stack[stackIdx].Get("name").String()
}

// a test implementation of a delivery (always returns a static message)
// ---

type testDelivery struct {
	attempt int
	queue   string
	msg     *events.Message
}

func newTestDelivery(attempt int, queue string, rollbackStackSize int) *testDelivery {
	// construct the rollback stack
	rollbackStack := newRollbackStack()
	for i := 0; i < rollbackStackSize; i += 1 {
		name := fmt.Sprintf("rollback_%d", i)
		rollbackMsg := newTaskMessage(newTestTask(name, name))
		rollbackStack.push(rollbackMsg.asRollbackStackItem())
	}
	// construct the task message
	taskMsg := newTaskMessageWithRollbackHistory(newTestTask("test", "test"), rollbackStack)
	msg, err := events.NewMessage(fmt.Sprintf("tasks.%s", taskMsg.TaskName()), taskMsg)
	if err != nil {
		panic(err)
	}
	return &testDelivery{attempt: attempt, queue: queue, msg: msg}
}

func newTestDeliveryRaw(attempt int, queue string, msg *events.Message) *testDelivery {
	return &testDelivery{attempt: attempt, queue: queue, msg: msg}
}

func (d *testDelivery) GetAttempt() int {
	return d.attempt
}

func (d *testDelivery) GetQueue() string {
	return d.queue
}

func (d *testDelivery) GetMessage() *events.Message {
	return d.msg
}
