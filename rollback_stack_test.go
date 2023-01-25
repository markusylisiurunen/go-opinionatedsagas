package opinionatedsagas

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRollbackStack(t *testing.T) {
	t.Run("pushes and pops a set of items", func(t *testing.T) {
		newItemWithName := func(name string) *rollbackStackItem {
			return &rollbackStackItem{name, map[string]any{}, map[string]any{}}
		}
		stack := &rollbackStack{}
		// push the items to the stack
		stack.push(newItemWithName("1"))
		stack.push(newItemWithName("2"))
		stack.push(newItemWithName("3"))
		// pop the items one by one
		first, ok := stack.pop()
		assert.NotNil(t, first)
		assert.True(t, ok)
		assert.Equal(t, "3", first.Name)
		second, ok := stack.pop()
		assert.NotNil(t, second)
		assert.True(t, ok)
		assert.Equal(t, "2", second.Name)
		third, ok := stack.pop()
		assert.NotNil(t, third)
		assert.True(t, ok)
		assert.Equal(t, "1", third.Name)
		// make sure an empty stack returns correctly
		_, ok = stack.pop()
		assert.False(t, ok)
	})

	t.Run("pushes and copies the stack", func(t *testing.T) {
		newItemWithName := func(name string) *rollbackStackItem {
			return &rollbackStackItem{name, map[string]any{}, map[string]any{}}
		}
		stackOne := &rollbackStack{}
		// push the items to the stack
		stackOne.push(newItemWithName("1"))
		stackOne.push(newItemWithName("2"))
		// push the last and copy the stack
		stackTwo := stackOne.copyAndPush(newItemWithName("3"))
		// validate the result
		assert.Len(t, stackOne.stack, 2)
		assert.Len(t, stackTwo.stack, 3)
		firstFromOne, ok := stackOne.pop()
		assert.True(t, ok)
		assert.Equal(t, "2", firstFromOne.Name)
		firstFromTwo, ok := stackTwo.pop()
		assert.True(t, ok)
		assert.Equal(t, "3", firstFromTwo.Name)
		secondFromTwo, ok := stackTwo.pop()
		assert.True(t, ok)
		assert.Equal(t, "2", secondFromTwo.Name)
	})

	t.Run("marshals and unmarshals correctly", func(t *testing.T) {
		newItemWithName := func(name string) *rollbackStackItem {
			return &rollbackStackItem{name, map[string]any{}, map[string]any{
				"original_name": name,
			}}
		}
		stack := &rollbackStack{}
		// push the items to the stack
		stack.push(newItemWithName("1"))
		stack.push(newItemWithName("2"))
		stack.push(newItemWithName("3"))
		// marshal the stack
		serialized, err := json.Marshal(stack)
		assert.NoError(t, err)
		// unmarshal it
		stack = &rollbackStack{}
		err = json.Unmarshal(serialized, stack)
		assert.NoError(t, err)
		// validate the stack's content
		assert.Len(t, stack.stack, 3)
		first, _ := stack.pop()
		second, _ := stack.pop()
		third, _ := stack.pop()
		assert.Equal(t, "3", first.Name)
		assert.Equal(t, "3", first.Task["original_name"])
		assert.Equal(t, "2", second.Name)
		assert.Equal(t, "2", second.Task["original_name"])
		assert.Equal(t, "1", third.Name)
		assert.Equal(t, "1", third.Task["original_name"])
	})
}
