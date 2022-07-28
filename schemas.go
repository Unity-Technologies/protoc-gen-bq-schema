package main

type BQSchema []*BQField

type BQField struct {
	Name        string      `json:"Name"`
	Type        string      `json:"type"`
	Mode        string      `json:"mode"`
	Description string      `json:"description,omitempty"`
	Fields      BQSchema    `json:"fields,omitempty"`
	PolicyTags  *PolicyTags `json:"policyTags,omitempty"`
}

type BQOption func(field *BQField)

func WithFields(fields BQSchema) BQOption {
	return func(field *BQField) {
		field.Fields = fields
	}
}

func NewBQField(name string, type_ string, mode string, description string, opts ...BQOption) *BQField {
	f := &BQField{Name: name, Type: type_, Mode: mode, Description: description}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// PolicyTags describes the structure of a Policy Tag
type PolicyTags struct {
	Names []string `json:"names,omitempty"`
}
