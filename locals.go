package main

import (
	"strings"

	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

// ProtoPackage describes a package of Protobuf, which is an container of message packages.
type ProtoPackage struct {
	Name     string
	parent   *ProtoPackage
	types    []*descriptor.DescriptorProto
	Index    map[string]*descriptor.DescriptorProto
	enums    map[string]*descriptor.EnumDescriptorProto
	comments map[string]Comments
}

func (p *ProtoPackage) Get(typeName string) *descriptor.DescriptorProto {
	n := strings.Split(typeName, ".")
	return p.Index[n[len(n)-1]]
}

func (p *ProtoPackage) traverse() {
	stack := append([]*descriptor.DescriptorProto{}, p.types...)
	for len(stack) > 0 {
		descriptorProto := stack[0]
		stack = stack[1:]
		p.Index[descriptorProto.GetName()] = descriptorProto
		stack = append(stack, descriptorProto.GetNestedType()...)
	}
}

type Locals struct {
	packages map[string]*ProtoPackage
	enums    map[string]*descriptor.EnumDescriptorProto
}

func (l *Locals) Set(key string, value *ProtoPackage) {
	l.packages[key] = value
}

func (l Locals) GetPackage(key string) (value *ProtoPackage) {
	return l.packages[key]
}

func (l Locals) GetTypeFromPackage(pkgName, key string) (value *descriptor.DescriptorProto) {
	return l.GetPackage(pkgName).Get(key)
}

func InitLocals(req *plugin.CodeGeneratorRequest) Locals {
	l := Locals{
		packages: make(map[string]*ProtoPackage, 0),
		enums:    make(map[string]*descriptor.EnumDescriptorProto, 0),
	}
	params := ParseRequestOptions(req.GetParameter())
	for _, file := range req.GetProtoFile() {
		handleSingleMessageOpt(file, req.GetParameter())
		if _, ok := params[file.GetName()]; file.GetPackage() == "" && ok {
			file.Package = proto.String(params[file.GetName()])
		}
		if pkg := l.GetPackage(file.GetPackage()); pkg == nil {
			p := &ProtoPackage{
				Name:     file.GetPackage(),
				parent:   nil,
				types:    file.GetMessageType(),
				comments: make(map[string]Comments),
				Index:    map[string]*descriptor.DescriptorProto{},
			}
			l.Set(file.GetPackage(), p)
		}
	}
	for _, protoPackage := range l.packages {
		protoPackage.traverse()
	}
	return l
}
