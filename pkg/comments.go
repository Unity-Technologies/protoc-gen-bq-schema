package pkg

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/golang/glog"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

const (
	messagePath    = 4 // FileDescriptorProto.message_type
	fieldPath      = 2 // DescriptorProto.field
	subMessagePath = 3 // DescriptorProto.nested_type
)

// {'4.0.2.0': 'ts',
// '4.0.2.1': 'product_slug',
// '4.0.2.2': 'event_type',
// '4.0.2.3': 'variable_data',
// '4.0.2.4': 'recipient',
// '4.0.2.5': 'service_id',
// '4.0.3.0.2.0': 'user_id',
// '4.0.3.0.2.1': 'org_id',
// '4.0.3.0.2.2': 'email',
// '4.0.3.0.2.3': 'locale',
// '4.0.3.0.2.4': 'first_name',
// '4.0.3.1.2.0': 'fields',
// '4.0.3.2.2.0': 'string_value',
// '4.0.3.2.2.1': 'list_value',
// '4.0.3.3.2.0': 'values'}

// Comments is a map between path in FileDescriptorProto and leading/trailing comments for each field.
type Comments map[string]string

// ParseComments reads FileDescriptorProto and parses comments into a map.
func ParseComments(fd *descriptor.FileDescriptorProto) Comments {
	comments := make(Comments)

	for _, loc := range fd.GetSourceCodeInfo().GetLocation() {
		if !hasComment(loc) {
			continue
		}

		path := loc.GetPath()
		key := make([]string, len(path))
		for idx, p := range path {
			key[idx] = strconv.FormatInt(int64(p), 10)
		}

		comments[strings.Join(key, ".")] = buildComment(loc)
	}
	js, _ := json.MarshalIndent(comments, "", " ")
	glog.Error(string(js))
	return comments
}

// Get returns comment for path or empty string if path has no comment.
func (c Comments) Get(path string) string {
	if val, ok := c[path]; ok {
		return val
	}

	return ""
}

func hasComment(loc *descriptor.SourceCodeInfo_Location) bool {
	if loc.GetLeadingComments() == "" && loc.GetTrailingComments() == "" {
		return false
	}

	return true
}

func buildComment(loc *descriptor.SourceCodeInfo_Location) string {
	comment := strings.TrimSpace(loc.GetLeadingComments()) + "\n\n" + strings.TrimSpace(loc.GetTrailingComments())
	return strings.Trim(comment, "\n")
}
