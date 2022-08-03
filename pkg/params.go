package pkg

import (
	"strings"
)

type Params map[string]string

func ParseRequestOptions(requestParam string) Params {
	p := make(Params)
	for _, s2 := range strings.Split(requestParam, ",") {
		if s2[0] == 'M' {
			parts := strings.Split(s2[1:], "=")
			p[parts[0]] = parts[1]
		}
	}
	return p
}
