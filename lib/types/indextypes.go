package types

import (
	"fmt"
	"strconv"
)

type IndexTypeMapping struct {
	Namespace string
	Index     string
	Type      string
}

type IndexingMeta struct {
	Routing         string
	Index           string
	Type            string
	Parent          string
	Version         int64
	VersionType     string
	TTL             string
	Pipeline        string
	RetryOnConflict int
}

func (meta *IndexingMeta) load(metaAttrs map[string]interface{}) {
	var v interface{}
	var ok bool
	var s string
	if v, ok = metaAttrs["routing"]; ok {
		meta.Routing = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["index"]; ok {
		meta.Index = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["type"]; ok {
		meta.Type = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["parent"]; ok {
		meta.Parent = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["version"]; ok {
		s = fmt.Sprintf("%v", v)
		if version, err := strconv.ParseInt(s, 10, 64); err == nil {
			meta.Version = version
		}
	}
	if v, ok = metaAttrs["versionType"]; ok {
		meta.VersionType = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["ttl"]; ok {
		meta.TTL = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["pipeline"]; ok {
		meta.Pipeline = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["retryOnConflict"]; ok {
		s = fmt.Sprintf("%v", v)
		if roc, err := strconv.Atoi(s); err == nil {
			meta.RetryOnConflict = roc
		}
	}
}

func (meta *IndexingMeta) shouldSave() bool {
	return (meta.Routing != "" ||
		meta.Index != "" ||
		meta.Type != "" ||
		meta.Parent != "" ||
		meta.Pipeline != "")
}
