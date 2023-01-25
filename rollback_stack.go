package opinionatedsagas

import "encoding/json"

type rollbackStackItem struct {
	Name string         `json:"name"`
	Meta map[string]any `json:"meta"`
	Task map[string]any `json:"task"`
}

type rollbackStack struct {
	stack []*rollbackStackItem
}

func newRollbackStack() *rollbackStack {
	return &rollbackStack{stack: []*rollbackStackItem{}}
}

func (s *rollbackStack) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.stack)
}

func (s *rollbackStack) UnmarshalJSON(data []byte) error {
	if s.stack == nil {
		s.stack = []*rollbackStackItem{}
	}
	return json.Unmarshal(data, &s.stack)
}

func (s *rollbackStack) copy() *rollbackStack {
	return &rollbackStack{stack: append([]*rollbackStackItem{}, s.stack...)}
}

func (s *rollbackStack) copyAndPush(item *rollbackStackItem) *rollbackStack {
	return &rollbackStack{stack: append([]*rollbackStackItem{item}, s.stack...)}
}

func (s *rollbackStack) push(item *rollbackStackItem) {
	s.stack = append([]*rollbackStackItem{item}, s.stack...)
}

func (s *rollbackStack) pop() (*rollbackStackItem, bool) {
	if len(s.stack) == 0 {
		return nil, false
	}
	first, rest := s.stack[0], s.stack[1:]
	s.stack = rest
	return first, true
}
