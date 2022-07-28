// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// protoc plugin which converts .proto to schema for BigQuery.
// It is spawned by protoc and generates schema for BigQuery, encoded in JSON.
//
// usage:
//  $ bin/protoc --bq-schema_out=path/to/outdir foo.proto
//

// Protobuf code for extensions are generated --
//go:generate protoc --go_out=. --go_opt=module=github.com/GoogleCloudPlatform/protoc-gen-bq-schema bq_table.proto bq_field.proto

package main

import (
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/protoc-gen-bq-schema/protos"

	"github.com/golang/glog"

	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

// Field describes the schema of a field in BigQuery.
type Field struct {
	Name        string      `json:"Name"`
	Type        string      `json:"type"`
	Mode        string      `json:"mode"`
	Description string      `json:"description,omitempty"`
	Fields      []*Field    `json:"fields,omitempty"`
	PolicyTags  *PolicyTags `json:"policyTags,omitempty"`
}

var (
	typeFromWKT = map[string]string{
		".google.protobuf.Int32Value":  "INTEGER",
		".google.protobuf.Int64Value":  "INTEGER",
		".google.protobuf.UInt32Value": "INTEGER",
		".google.protobuf.UInt64Value": "INTEGER",
		".google.protobuf.DoubleValue": "FLOAT",
		".google.protobuf.FloatValue":  "FLOAT",
		".google.protobuf.BoolValue":   "BOOLEAN",
		".google.protobuf.StringValue": "STRING",
		".google.protobuf.BytesValue":  "BYTES",
		".google.protobuf.Duration":    "STRING",
		".google.protobuf.Timestamp":   "TIMESTAMP",
	}
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

func convertField(
  curPkg *ProtoPackage,
  desc *descriptor.FieldDescriptorProto,
  msgOpts *protos.BigQueryMessageOptions,
  parentMessages map[*descriptor.DescriptorProto]bool,
  comments Comments,
  path string) (*Field, error) {

	field := &Field{
		Name: desc.GetName(),
	}
	if msgOpts.GetUseJsonNames() && desc.GetJsonName() != "" {
		field.Name = desc.GetJsonName()
	}

	var ok bool
	field.Mode, ok = modeFromFieldLabel[desc.GetLabel()]
	if !ok {
		return nil, fmt.Errorf("unrecognized field label: %s", desc.GetLabel().String())
	}

	field.Type, ok = typeFromFieldType[desc.GetType()]
	if !ok {
		return nil, fmt.Errorf("unrecognized field type: %s", desc.GetType().String())
	}

	if comment := comments.Get(path); comment != "" {
		field.Description = comment
	}

	opts := desc.GetOptions()
	if opts != nil && proto.HasExtension(opts, protos.E_Bigquery) {
		opt := proto.GetExtension(opts, protos.E_Bigquery).(*protos.BigQueryFieldOptions)
		if opt.Ignore {
			// skip the field below
			return nil, nil
		}

		if opt.Require {
			field.Mode = "REQUIRED"
		}

		if len(opt.TypeOverride) > 0 {
			field.Type = opt.TypeOverride
		}

		if len(opt.Name) > 0 {
			field.Name = opt.Name
		}

		if len(opt.Description) > 0 {
			field.Description = opt.Description
		}

		if len(opt.PolicyTags) > 0 {
			field.PolicyTags = &PolicyTags{
				Names: []string{opt.PolicyTags},
			}
		}
	}

	if field.Type != "RECORD" {
		return field, nil
	}
	if t, ok := typeFromWKT[desc.GetTypeName()]; ok {
		field.Type = t
		return field, nil
	}

	// fields, err := convertFieldsForType(curPkg, desc.GetTypeName(), parentMessages)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// if len(fields) == 0 { // discard RECORDs that would have zero fields
	// 	return nil, nil
	// }
	//
	// field.Fields = fields

	return field, nil
}

func convertExtraField(curPkg *ProtoPackage, extraFieldDefinition string, parentMessages map[*descriptor.DescriptorProto]bool) (*Field, error) {
	parts := strings.Split(extraFieldDefinition, ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("expecting at least 2 parts in extra field definition separated by colon, got %d", len(parts))
	}

	field := &Field{
		Name: parts[0],
		Type: parts[1],
		Mode: "NULLABLE",
	}

	modeIndex := 2
	if field.Type == "RECORD" {
		modeIndex = 3
	}
	if len(parts) > modeIndex {
		field.Mode = parts[modeIndex]
	}

	if field.Type != "RECORD" {
		return field, nil
	}

	if len(parts) < 3 {
		return nil, fmt.Errorf("extra field %s has no type defined", field.Type)
	}

	typeName := parts[2]

	if t, ok := typeFromWKT[typeName]; ok {
		field.Type = t
		return field, nil
	}

	// fields := []*Field
	//
	// if len(fields) == 0 { // discard RECORDs that would have zero fields
	// 	return nil, nil
	// }
	//
	// field.Fields = fields

	return field, nil
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
