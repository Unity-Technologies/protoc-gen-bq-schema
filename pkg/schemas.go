package pkg

import (
	"fmt"
)

type Schema []*Field

type Field struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	Mode        string      `json:"mode"`
	Description string      `json:"description,omitempty"`
	Fields      Schema      `json:"fields,omitempty"`
	PolicyTags  *PolicyTags `json:"policyTags,omitempty"`
}

func (b *Field) String() string {
	return fmt.Sprintf("<Field: %s %s %s>", b.Mode, b.Name, b.Type)
}

type BQOption func(field *Field)

func WithFields(fields Schema) BQOption {
	return func(field *Field) {
		field.Fields = fields
	}
}

func NewBQField(name string, type_ string, mode string, description string, opts ...BQOption) *Field {
	f := &Field{Name: name, Type: type_, Mode: mode, Description: description}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// PolicyTags describes the structure of a Policy Tag
type PolicyTags struct {
	Names []string `json:"names,omitempty"`
}
