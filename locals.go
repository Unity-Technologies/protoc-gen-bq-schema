package main

import (
	"strings"

	"github.com/golang/glog"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

// ProtoPackage describes a package of Protobuf, which is an container of message packages.
type ProtoPackage struct {
	name     string
	parent   *ProtoPackage
	types    []*descriptor.DescriptorProto
	index    map[string]*descriptor.DescriptorProto
	enums    map[string]*descriptor.EnumDescriptorProto
	comments map[string]Comments
}

func (p *ProtoPackage) traverse() {
	stack := append([]*descriptor.DescriptorProto{}, p.types...)
	prefix := p.name
	for len(stack) > 0 {
		descriptorProto := stack[0]
		stack = stack[1:]
		key := strings.Join([]string{prefix, descriptorProto.GetName()}, ".")
		prefix = key
		p.index[key] = descriptorProto
		stack = append(stack, descriptorProto.GetNestedType()...)
	}
	glog.Error(p)
}

type Locals struct {
	packages map[string]*ProtoPackage
	enums    map[string]*descriptor.EnumDescriptorProto
}

func (l *Locals) Set(key string, value *ProtoPackage) {
	l.packages[key] = value
}

func (l Locals) GetType(key string) (value *ProtoPackage) {
	return l.packages[key]
}

func (l *Locals) Init(req *plugin.CodeGeneratorRequest) {
	if l.packages == nil {
		l.packages = make(map[string]*ProtoPackage, 0)
		l.enums = make(map[string]*descriptor.EnumDescriptorProto, 0)
	}
	glog.Error(l.packages == nil)
	params := ParseRequestOptions(req.GetParameter())
	for _, file := range req.GetProtoFile() {
		handleSingleMessageOpt(file, req.GetParameter())
		if _, ok := params[file.GetName()]; file.GetPackage() == "" && ok {
			file.Package = proto.String(params[file.GetName()])
		}
		if pkg := l.GetType(file.GetPackage()); pkg == nil {
			p := &ProtoPackage{
				name:     file.GetPackage(),
				parent:   nil,
				types:    file.GetMessageType(),
				comments: make(map[string]Comments),
				index:    map[string]*descriptor.DescriptorProto{},
			}
			l.Set(file.GetPackage(), p)
		}
	}
	for _, protoPackage := range l.packages {
		protoPackage.traverse()
	}
}
