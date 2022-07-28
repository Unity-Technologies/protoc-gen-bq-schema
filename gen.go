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

	"github.com/GoogleCloudPlatform/protoc-gen-bq-schema/protos"
	"github.com/golang/glog"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

// GetCodeGenRequest accepts an `io.Reader` and reads the entire stream; unmarshalling the data into a `CodeGeneratorRequest`. This request is used to generate the BQ schema
func GetCodeGenRequest(rd io.Reader) (*plugin.CodeGeneratorRequest, error) {
	glog.Info("convertFrom")
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
	return req, err
}

func convert(req *plugin.CodeGeneratorRequest) (*plugin.CodeGeneratorResponse, error) {
	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}
	res := &plugin.CodeGeneratorResponse{}
	for _, file := range req.GetProtoFile() {
		if _, ok := generateTargets[file.GetName()]; ok {
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
	var opts *protos.BigQueryMessageOptions
	var err error

	glog.Info("convertFile")
	name := path.Base(file.GetName())

	comments := ParseComments(file)
	response := make([]*plugin.CodeGeneratorResponse_File, 0)
	for _, msg := range file.GetMessageType() {
		// p := fmt.Sprintf("%d.%d", messagePath, msgIndex)

		if opts, err = getBigqueryMessageOptions(msg); err != nil {
			return nil, err
		}
		if opts == nil {
			glog.Error("wtf")
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

		jsonSchema, err := json.MarshalIndent([]byte{}, "", " ")
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
	var locals Locals
	var req *plugin.CodeGeneratorRequest
	var res *plugin.CodeGeneratorResponse
	var data []byte
	var err error

	flag.Parse()
	if req, err = GetCodeGenRequest(os.Stdin); err != nil {
		if res == nil {
			message := fmt.Sprintf("Failed to read input: %v", err)
			res = &plugin.CodeGeneratorResponse{
				Error: &message,
			}
		}
	}
	locals.Init(req)
	if res, err = convert(req); err != nil {
		glog.Errorf("Cannot convert request: %v", err)
		return
	}
	if data, err = proto.Marshal(res); err != nil {
		glog.Errorf("Cannot marshal response: %v", err)
		return
	}
	if _, err = os.Stdout.Write(data); err != nil {
		glog.Errorf("Failed to write response: %v", err)
		return
	}
}
