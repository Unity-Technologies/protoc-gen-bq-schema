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

func getNested(pkgName string, fieldProto *descriptor.FieldDescriptorProto) *descriptor.DescriptorProto {
	n := strings.Split(fieldProto.GetTypeName(), ".")
	return locals.GetTypeFromPackage(pkgName, n[len(n)-1])
}

func IsRecordType(fieldProto *descriptor.FieldDescriptorProto) bool {
	return fieldProto.GetType() == descriptor.FieldDescriptorProto_TYPE_GROUP || fieldProto.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE
}

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

var seen = map[string]bool{}

func _traverseField(pkg *ProtoPackage, bqField *BQField, protoField *descriptor.FieldDescriptorProto, descriptor *descriptor.DescriptorProto, level int) *BQField {
	if IsRecordType(protoField) {
		level += 1
		descriptor = getNested(pkg.Name, protoField)
		for _, inner := range descriptor.GetField() {
			if _, found := seen[inner.GetName()]; !found {
				innerBQField := NewBQField(
					inner.GetJsonName(),
					typeFromFieldType[inner.GetType()],
					modeFromFieldLabel[inner.GetLabel()],
					"",
				)
				bqField.Fields = append(bqField.Fields, innerBQField)
				if _, ok := seen[innerBQField.Name]; IsRecordType(inner) && !ok {
					seen[innerBQField.Name] = true
					bqField = _traverseField(pkg, innerBQField, inner, descriptor, level)
				}
			}
		}
	}
	return bqField
}

func traverseFields(pkgName string, msg *descriptor.DescriptorProto) BQSchema {
	schema := make(BQSchema, 0)
	pkg := locals.GetPackage(pkgName)
	var bqField *BQField
	fields := msg.GetField()
	for _, fieldProto := range fields {
		bqField = NewBQField(
			fieldProto.GetJsonName(),
			typeFromFieldType[fieldProto.GetType()],
			modeFromFieldLabel[fieldProto.GetLabel()],
			"",
		)
		if IsRecordType(fieldProto) {
			bqField = _traverseField(pkg, bqField, fieldProto, getNested(pkg.Name, fieldProto), 0)
		}
		schema = append(schema, bqField)
	}
	return schema
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
	schema := traverseFields(pkgName, msg)

	if jsonSchema, err = json.MarshalIndent(schema, "", " "); err != nil {
		return nil, err
	}
	resFile := &plugin.CodeGeneratorResponse_File{
		Name:    proto.String(fmt.Sprintf("%s/%s.schema", strings.Replace(pkgName, ".", "/", -1), tableName)),
		Content: proto.String(string(jsonSchema)),
	}
	return resFile, nil
}

func getFilesForResponse(file *descriptor.FileDescriptorProto) ([]*plugin.CodeGeneratorResponse_File, error) {
	var f *plugin.CodeGeneratorResponse_File
	var err error

	// comments := ParseComments(file)
	responseFiles := make([]*plugin.CodeGeneratorResponse_File, 0)

	for _, msg := range file.GetMessageType() {
		if f, err = getFileForResponse(file.GetPackage(), msg); err != nil {
			return nil, err
		}
		responseFiles = append(responseFiles, f)
	}
	return responseFiles, nil
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
	locals = InitLocals(req)
	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}

	params := ParseRequestOptions(req.GetParameter())
	for _, file := range req.GetProtoFile() {
		handleSingleMessageOpt(file, req.GetParameter())
		if _, ok := params[file.GetName()]; file.GetPackage() == "" && ok {
			file.Package = proto.String(params[file.GetName()])
		}
		if _, ok := generateTargets[file.GetName()]; ok {
			if converted, err = getFilesForResponse(file); err != nil {
				res.Error = proto.String(fmt.Sprintf("Failed to convert %s: %v", file.GetName(), err))
			}
			res.File = append(res.File, converted...)
		}
	}

	writeResp(res)
}
