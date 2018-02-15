package index

var MapIndexTypes map[string]*IndexTypeMapping

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
