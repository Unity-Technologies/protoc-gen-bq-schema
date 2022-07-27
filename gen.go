package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/golang/glog"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

type Locals map[string]*descriptor.DescriptorProto

type PPackage struct {
	name     string
	types    map[string]*descriptor.DescriptorProto
	comments map[string]Comments
}

type ProtoField struct {
	name string
}

type ProtoMessage struct {
	name   string
	fields []*ProtoField
}

func convertFrom(rd io.Reader) (*plugin.CodeGeneratorResponse, error) {
	glog.Info("convertFrom")
	glog.V(1).Info("Reading code generation request")
	input, err := ioutil.ReadAll(rd)
	if err != nil {
		glog.Error("Failed to read request:", err)
		return nil, err
	}
	req := &plugin.CodeGeneratorRequest{}
	err = proto.Unmarshal(input, req)
	if err != nil {
		glog.Error("Can't unmarshal input:", err)
		return nil, err
	}

	glog.V(1).Info("Converting input")
	return convert(req)
}

func convert(req *plugin.CodeGeneratorRequest) (*plugin.CodeGeneratorResponse, error) {
	glog.Info("convert")
	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}
	params := parseRequestOptions(req.GetParameter())
	for _, file := range req.GetProtoFile() {
		if _, ok := params[file.GetName()]; file.GetPackage() == "" && ok {
			p := params[file.GetName()]
			file.Package = &p
			for _, descriptorProto := range file.GetMessageType() {
				for _, field := range descriptorProto.GetField() {
					tmp := fmt.Sprintf(".%s%s", file.GetPackage(), field.GetTypeName())
					field.TypeName = &tmp
					glog.Infof("Name: %25s\tType: %15s", field.GetName(), field.GetTypeName())
				}
				glog.Info()

				for _, d := range descriptorProto.GetNestedType() {
					glog.Infof("Name: %-25s", d.GetName())
					for _, nestedDesc := range d.GetField() {
						tmp := fmt.Sprintf(".%s%s", file.GetPackage(), nestedDesc.GetTypeName())
						nestedDesc.TypeName = &tmp
						glog.Infof("Name: %25s\tType: %15s", nestedDesc.GetName(), nestedDesc.GetTypeName())
					}
					glog.Info()
				}
			}
		}
	}
	res := &plugin.CodeGeneratorResponse{}
	for _, file := range req.GetProtoFile() {
		for msgIndex, msg := range file.GetMessageType() {
			glog.V(1).Infof("Loading a message type %s from package %s", msg.GetName(), file.GetPackage())
			registerType(file.Package, msg, ParseComments(file), fmt.Sprintf("%d.%d", messagePath, msgIndex))
		}
	}
	for _, file := range req.GetProtoFile() {
		if _, ok := generateTargets[file.GetName()]; ok {
			glog.V(1).Info("Converting ", file.GetName())
			handleSingleMessageOpt(file, req.GetParameter())
			// TODO:  printStuff(file)
			converted, err := convertFile(file)
			if err != nil {
				res.Error = proto.String(fmt.Sprintf("Failed to convert %s: %v", file.GetName(), err))
				return res, err
			}
			res.File = append(res.File, converted...)
		}
	}
	return res, nil
}

func convertFile(file *descriptor.FileDescriptorProto) ([]*plugin.CodeGeneratorResponse_File, error) {
	glog.Info("convertFile")
	name := path.Base(file.GetName())
	pkg, ok := globalPkg.relativelyLookupPackage(file.GetPackage())
	if !ok {
		return nil, fmt.Errorf("no such package found: %s", file.GetPackage())
	}

	comments := ParseComments(file)
	response := make([]*plugin.CodeGeneratorResponse_File, 0)
	for msgIndex, msg := range file.GetMessageType() {
		p := fmt.Sprintf("%d.%d", messagePath, msgIndex)

		opts, err := getBigqueryMessageOptions(msg)
		if err != nil {
			return nil, err
		}
		if opts == nil {
			continue
		}

		tableName := opts.GetTableName()
		if len(tableName) == 0 {
			continue
		}

		glog.Errorf("Generating schema for a message type %s", msg.GetName())
		schema, err := convertMessageType(pkg, msg, opts, make(map[*descriptor.DescriptorProto]bool), comments, p)
		if err != nil {
			glog.Errorf("Failed to convert %s: %v", name, err)
			return nil, err
		}

		jsonSchema, err := json.MarshalIndent(schema, "", " ")
		if err != nil {
			glog.Error("Failed to encode schema", err)
			return nil, err
		}

		resFile := &plugin.CodeGeneratorResponse_File{
			Name:    proto.String(fmt.Sprintf("%s/%s.schema", strings.Replace(file.GetPackage(), ".", "/", -1), tableName)),
			Content: proto.String(string(jsonSchema)),
		}
		response = append(response, resFile)
	}

	return response, nil
}

func main() {
	flag.Parse()
	ok := true
	glog.Info("Processing code generator request")
	res, err := convertFrom(os.Stdin)
	if err != nil {
		ok = false
		if res == nil {
			message := fmt.Sprintf("Failed to read input: %v", err)
			res = &plugin.CodeGeneratorResponse{
				Error: &message,
			}
		}
	}

	glog.Info("Serializing code generator response")
	data, err := proto.Marshal(res)
	if err != nil {
		glog.Fatal("Cannot marshal response", err)
	}
	_, err = os.Stdout.Write(data)
	if err != nil {
		glog.Fatal("Failed to write response", err)
	}

	if ok {
		glog.Info("Succeeded to process code generator request")
	} else {
		glog.Info("Failed to process code generator but successfully sent the error to protoc")
	}
}
