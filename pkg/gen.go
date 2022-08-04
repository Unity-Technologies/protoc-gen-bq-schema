package pkg

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

var (
	locals            Locals
	comments          Comments
	seen              = map[*descriptor.FieldDescriptorProto]bool{}
	typeFromFieldType = map[descriptor.FieldDescriptorProto_Type]string{
		descriptor.FieldDescriptorProto_TYPE_DOUBLE: "FLOAT",
		descriptor.FieldDescriptorProto_TYPE_FLOAT:  "FLOAT",

		descriptor.FieldDescriptorProto_TYPE_INT64:    "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_UINT64:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_INT32:    "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_UINT32:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_FIXED64:  "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_FIXED32:  "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SFIXED32: "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SFIXED64: "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SINT32:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SINT64:   "INTEGER",

		descriptor.FieldDescriptorProto_TYPE_STRING: "STRING",
		descriptor.FieldDescriptorProto_TYPE_BYTES:  "BYTES",
		descriptor.FieldDescriptorProto_TYPE_ENUM:   "STRING",

		descriptor.FieldDescriptorProto_TYPE_BOOL: "BOOLEAN",

		descriptor.FieldDescriptorProto_TYPE_GROUP:   "RECORD",
		descriptor.FieldDescriptorProto_TYPE_MESSAGE: "RECORD",
	}

	modeFromFieldLabel = map[descriptor.FieldDescriptorProto_Label]string{
		descriptor.FieldDescriptorProto_LABEL_OPTIONAL: "NULLABLE",
		descriptor.FieldDescriptorProto_LABEL_REQUIRED: "REQUIRED",
		descriptor.FieldDescriptorProto_LABEL_REPEATED: "REPEATED",
	}
)

func getNested(pkgName string, fieldProto *descriptor.FieldDescriptorProto) *ProtoType {
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

func _traverseField(pkgName string, bqField *BQField, protoField *descriptor.FieldDescriptorProto, desc *descriptor.DescriptorProto, parentMessages map[*descriptor.DescriptorProto]bool) *BQField {
	if IsRecordType(protoField) {
		pt := getNested(pkgName, protoField)
		desc = pt.Type
		for idx, inner := range desc.GetField() {
			if _, found := parentMessages[desc]; !found {
				fieldCommentPath := fmt.Sprintf("%s.%d.%d", pt.Path, fieldPath, idx)
				innerBQField := NewBQField(
					inner.GetName(),
					typeFromFieldType[inner.GetType()],
					modeFromFieldLabel[inner.GetLabel()],
					comments[fieldCommentPath],
				)
				if _, ok := parentMessages[desc]; !ok {
					if IsRecordType(inner) {
						innerBQField = _traverseField(pkgName, innerBQField, inner, desc, parentMessages)
						bqField.Fields = append(bqField.Fields, innerBQField)
					} else {
						bqField.Fields = append(bqField.Fields, innerBQField)
					}
				}
			}
		}
	}
	return bqField
}

func traverseMessage(pkgName string, msg *descriptor.DescriptorProto, path string, parentMessages map[*descriptor.DescriptorProto]bool) BQSchema {
	var bqField *BQField
	schema := make(BQSchema, 0)
	fields := msg.GetField()
	parentMessages[msg] = true
	for idx, fieldProto := range fields {
		fieldCommentPath := fmt.Sprintf("%s.%d.%d", path, fieldPath, idx)
		bqField = NewBQField(
			fieldProto.GetName(),
			typeFromFieldType[fieldProto.GetType()],
			modeFromFieldLabel[fieldProto.GetLabel()],
			comments[fieldCommentPath],
		)
		if IsRecordType(fieldProto) {
			bqField = _traverseField(pkgName, bqField, fieldProto, getNested(pkgName, fieldProto).Type, parentMessages)
		}
		schema = append(schema, bqField)
	}
	parentMessages[msg] = false
	return schema
}

func getFileForResponse(pkgName string, msg *descriptor.DescriptorProto, path string, parentMessages map[*descriptor.DescriptorProto]bool) (*plugin.CodeGeneratorResponse_File, error) {
	var opts *protos.BigQueryMessageOptions
	var jsonSchema []byte
	var err error

	if opts, err = getBigqueryMessageOptions(msg); err != nil {
		return nil, err
	}
	tableName := opts.GetTableName()
	schema := traverseMessage(pkgName, msg, path, parentMessages)

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

	responseFiles := make([]*plugin.CodeGeneratorResponse_File, 0)
	for _, msg := range file.GetMessageType() {
		pt := locals.GetTypeFromPackage(file.GetPackage(), msg.GetName())
		if f, err = getFileForResponse(file.GetPackage(), msg, pt.Path, map[*descriptor.DescriptorProto]bool{}); err != nil {
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

// handleSingleMessageOpt handles --bq-schema_opt=single-message in protoc params.
// providing that param tells protoc-gen-bq-schema to treat each proto files only contains one top-level type.
// if a file contains no message packages, then this function simply does nothing.
// if a file contains more than one message packages, then only the first message type will be processed.
// in that case, the table names will follow the proto file names.
func handleSingleMessageOpt(file *descriptor.FileDescriptorProto, requestParam string) {
	glog.Info("handleSingleMessageOpt")
	if !strings.Contains(requestParam, "single-message") || len(file.GetMessageType()) == 0 {
		return
	}
	file.MessageType = file.GetMessageType()[:1]
	message := file.GetMessageType()[0]
	message.Options = &descriptor.MessageOptions{}
	fileName := file.GetName()
	proto.SetExtension(message.GetOptions(), protos.E_BigqueryOpts, &protos.BigQueryMessageOptions{
		TableName: fileName[strings.LastIndexByte(fileName, '/')+1 : strings.LastIndexByte(fileName, '.')],
	})
}

// getBigqueryMessageOptions returns the bigquery options for the given message.
// If an error is encountered, it is returned instead. If no error occurs, but
// the message has no gen_bq_schema.bigquery_opts option, this function returns
// nil, nil.
func getBigqueryMessageOptions(msg *descriptor.DescriptorProto) (*protos.BigQueryMessageOptions, error) {
	glog.Info("getBigqueryMessageOptions")
	options := msg.GetOptions()
	if options == nil {
		return nil, nil
	}

	if !proto.HasExtension(options, protos.E_BigqueryOpts) {
		return nil, nil
	}

	return proto.GetExtension(options, protos.E_BigqueryOpts).(*protos.BigQueryMessageOptions), nil
}

func Do() {
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
		comments = ParseComments(file)
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
