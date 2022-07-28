package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/protoc-gen-bq-schema/protos"
	"github.com/golang/glog"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

var locals Locals

// GetCodeGenRequestResponse accepts an `io.Reader` and reads the entire stream; unmarshalling the data into a
// `CodeGeneratorRequest`. This request is used to generate the BQ schema
func GetCodeGenRequestResponse(rd io.Reader) (*plugin.CodeGeneratorRequest, *plugin.CodeGeneratorResponse) {
	var input []byte
	var err error

	req := &plugin.CodeGeneratorRequest{}
	resp := &plugin.CodeGeneratorResponse{}
	if input, err = ioutil.ReadAll(rd); err != nil {
		resp.Error = proto.String(err.Error())
		return req, resp
	}
	if err = proto.Unmarshal(input, req); err != nil {
		resp.Error = proto.String(err.Error())
		return req, resp
	}
	return req, resp
}

func DottedName(root string, parts ...string) string {
	return strings.Join(append([]string{root}, parts...), ".")
}

func traverse(prefix string, stack []*descriptor.DescriptorProto) BQSchema {
	results := make(BQSchema)
	for len(stack) > 0 {
		descriptorProto := stack[0]
		stack = stack[1:]
		key := strings.Join([]string{prefix, descriptorProto.GetName()}, ".")
		prefix = key
		stack = append(stack, descriptorProto.GetNestedType()...)
		field := &BQField{
			Name:        descriptorProto.GetName(),
			Type:        DottedName(prefix, descriptorProto.GetName()),
			Mode:        "NULLABLE",
			Description: "",
			Fields:      nil,
			PolicyTags:  nil,
		}
	}
	glog.Error(p)
}

func getFileForResponse(pkgName string, msg *descriptor.DescriptorProto) (*plugin.CodeGeneratorResponse_File, error) {
	// p := fmt.Sprintf("%d.%d", messagePath, msgIndex)
	var opts *protos.BigQueryMessageOptions
	var jsonSchema []byte
	var err error

	if opts, err = getBigqueryMessageOptions(msg); err != nil {
		return nil, err
	}

	tableName := opts.GetTableName()
	if jsonSchema, err = json.MarshalIndent([]byte{}, "", " "); err != nil {
		return nil, err
	}
	stack := []*descriptor.DescriptorProto{msg}
	traverse(pkgName, stack)

	resFile := &plugin.CodeGeneratorResponse_File{
		Name:    proto.String(fmt.Sprintf("%s/%s.schema", strings.Replace(pkgName, ".", "/", -1), tableName)),
		Content: proto.String(string(jsonSchema)),
	}
	return resFile, nil
}

func getFilesForResponse(file *descriptor.FileDescriptorProto) ([]*plugin.CodeGeneratorResponse_File, error) {
	var err error

	// name := path.Base(file.GetName())
	// comments := ParseComments(file)
	// schema := make(BQSchema, 0)
	responseFile := make([]*plugin.CodeGeneratorResponse_File, 0)

	for _, msg := range file.GetMessageType() {
		responseFile = append(responseFile, getFileForResponse(msg))
	}
	return responseFile, nil
}

func writeResp(res *plugin.CodeGeneratorResponse) {
	var data []byte
	var err error

	if data, err = proto.Marshal(res); err != nil {
		glog.Errorf("cannot marshal response: %v", err)
	}
	if _, err = os.Stdout.Write(data); err != nil {
		glog.Errorf("failed to write response: %v", err)
	}
}

func main() {
	var req *plugin.CodeGeneratorRequest
	var res *plugin.CodeGeneratorResponse
	var converted []*plugin.CodeGeneratorResponse_File

	var err error

	flag.Parse()
	if req, res = GetCodeGenRequestResponse(os.Stdin); res.Error != nil {
		return
	}
	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}

	for _, file := range req.GetProtoFile() {
		if _, ok := generateTargets[file.GetName()]; ok {
			if converted, err = getFilesForResponse(file); err != nil {
				res.Error = proto.String(fmt.Sprintf("Failed to convert %s: %v", file.GetName(), err))
			}
			res.File = append(res.File, converted...)
		}
	}

	writeResp(res)
}
