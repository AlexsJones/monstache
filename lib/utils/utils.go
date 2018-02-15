package utils

import "strings"

func NormalizeIndexName(name string) (normal string) {
	normal = strings.ToLower(strings.TrimPrefix(name, "_"))
	return
}

func NormalizeTypeName(name string) (normal string) {
	normal = strings.TrimPrefix(name, "_")
	return
}

func NormalizeEsID(id string) (normal string) {
	normal = strings.TrimPrefix(id, "_")
	return
}
