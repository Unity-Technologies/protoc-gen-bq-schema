package pkg

import (
	"fmt"
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
	Index    map[string]*ProtoType
	Index2   map[string]*descriptor.DescriptorProto
	enums    map[string]*descriptor.EnumDescriptorProto
	comments map[string]Comments
}

type ProtoType struct {
	Type *descriptor.DescriptorProto
	Path string
}

func (p *ProtoPackage) Get(typeName string) *ProtoType {
	n := strings.Split(typeName, ".")
	return p.Index[n[len(n)-1]]
}

func (p *ProtoPackage) _traverse(path string, types []*descriptor.DescriptorProto) {
	for nestedIdx, nestedDesc := range types {
		innerPath := fmt.Sprintf("%s.%d.%d", path, subMessagePath, nestedIdx)
		nestedPT := &ProtoType{
			Type: nestedDesc,
			Path: innerPath,
		}
		p.Index[nestedDesc.GetName()] = nestedPT
		p._traverse(innerPath, nestedDesc.GetNestedType())
	}
}

func (p *ProtoPackage) traverse() {
	for idx, desc := range p.types {
		path := fmt.Sprintf("%d.%d", messagePath, idx)
		pt := &ProtoType{
			Type: desc,
			Path: path,
		}
		p.Index[desc.GetName()] = pt

		p._traverse(path, desc.GetNestedType())
	}
}

type Locals struct {
	packages map[string]*ProtoPackage
	enums    map[string]*descriptor.EnumDescriptorProto
}

func (l *Locals) Set(key string, value *ProtoPackage) {
	l.packages[key] = value
}

func (l *Locals) GetPackage(key string) (value *ProtoPackage) {
	return l.packages[key]
}

func (l *Locals) GetTypeFromPackage(pkgName, key string) (value *ProtoType) {
	tmp := l.GetPackage(pkgName).Get(key)
	return tmp
}

func InitLocals(req *plugin.CodeGeneratorRequest) Locals {
	l := Locals{
		packages: make(map[string]*ProtoPackage, 0),
		enums:    make(map[string]*descriptor.EnumDescriptorProto, 0),
	}
	params := ParseRequestOptions(req.GetParameter())
	for _, file := range req.GetProtoFile() {
		handleSingleMessageOpt(file, req.GetParameter())
		if _, ok := params[file.GetName()]; ok {
			file.Package = proto.String(params[file.GetName()])
		}
		if pkg := l.GetPackage(file.GetPackage()); pkg == nil {
			pkg = &ProtoPackage{
				Name:     file.GetPackage(),
				parent:   nil,
				types:    file.GetMessageType(),
				comments: make(map[string]Comments),
				Index:    map[string]*ProtoType{},
				Index2:   map[string]*descriptor.DescriptorProto{},
			}
			l.Set(file.GetPackage(), pkg)
		}
		for _, protoPackage := range l.packages {
			protoPackage.traverse()
		}
	}
	return l
}
