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
	locals   Locals
	comments Comments
	seen     = map[string]bool{}
)

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

func _traverseField(pkg *ProtoPackage, bqField *BQField, protoField *descriptor.FieldDescriptorProto, desc *descriptor.DescriptorProto, path string) *BQField {
	if IsRecordType(protoField) {
		desc = getNested(pkg.Name, protoField)
		for idx, inner := range desc.GetField() {
			if _, found := seen[inner.GetName()]; !found {
				var comment, fieldCommentPath string
				var ok bool

				fieldCommentPath = fmt.Sprintf("%s.%d.%d", path, subMessagePath, inner.GetNumber())
				if comment, ok = comments[fieldCommentPath]; !ok {
					if inner.GetTypeName() != "" {
						fieldCommentPath = fmt.Sprintf("%s.%d.%d", fieldCommentPath, fieldPath, idx)
						comment = comments[fieldCommentPath]
					}
				}
				if comment == "" {
					glog.Errorf("%s: %s", fieldCommentPath, inner.GetName())
				}
				innerBQField := NewBQField(
					inner.GetName(),
					typeFromFieldType[inner.GetType()],
					modeFromFieldLabel[inner.GetLabel()],
					comment,
				)
				if _, ok := seen[innerBQField.Name]; !ok {
					if IsRecordType(inner) {
						seen[innerBQField.Name] = true
						innerBQField = _traverseField(pkg, innerBQField, inner, desc, fieldCommentPath)
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

func traverseFields(pkgName string, msg *descriptor.DescriptorProto, path string) BQSchema {
	var bqField *BQField
	schema := make(BQSchema, 0)
	pkg := locals.GetPackage(pkgName)
	fields := msg.GetField()
	for idx, fieldProto := range fields {

		fieldCommentPath := fmt.Sprintf("%s.%d.%d", path, fieldPath, idx)
		bqField = NewBQField(
			fieldProto.GetName(),
			typeFromFieldType[fieldProto.GetType()],
			modeFromFieldLabel[fieldProto.GetLabel()],
			comments[fieldCommentPath],
		)
		if IsRecordType(fieldProto) {
			bqField = _traverseField(pkg, bqField, fieldProto, getNested(pkg.Name, fieldProto), path)
		}
		schema = append(schema, bqField)
	}
	return schema
}

func getFileForResponse(pkgName string, msg *descriptor.DescriptorProto, path string) (*plugin.CodeGeneratorResponse_File, error) {
	// p := fmt.Sprintf("%d.%d", messagePath, msgIndex)
	var opts *protos.BigQueryMessageOptions
	var jsonSchema []byte
	var err error

	if opts, err = getBigqueryMessageOptions(msg); err != nil {
		return nil, err
	}
	tableName := opts.GetTableName()
	schema := traverseFields(pkgName, msg, path)

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

	for msgIndex, msg := range file.GetMessageType() {
		path := fmt.Sprintf("%d.%d", messagePath, msgIndex)
		if f, err = getFileForResponse(file.GetPackage(), msg, path); err != nil {
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
