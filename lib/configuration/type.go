package configuration

import (
	g "github.com/AlexsJones/monstache/lib/gmt"
	"github.com/AlexsJones/monstache/lib/index"
	"github.com/AlexsJones/monstache/lib/log"
	"github.com/AlexsJones/monstache/lib/mongo"
	"github.com/AlexsJones/monstache/lib/types"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"github.com/rwynn/monstache/monstachemap"
)

var MapEnvs map[string]*executionEnv

var FileNamespaces map[string]bool

var PatchNamespaces map[string]bool

var MapperPlugin func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error)

type executionEnv struct {
	VM      *otto.Otto
	Script  string
	Routing bool
}

type javascript struct {
	Namespace string
	Script    string
	Routing   bool
}

type ConfigOptions struct {
	MongoURL                 string                     `toml:"mongo-url"`
	MongoConfigURL           string                     `toml:"mongo-config-url"`
	MongoPemFile             string                     `toml:"mongo-pem-file"`
	MongoValidatePemFile     bool                       `toml:"mongo-validate-pem-file"`
	MongoOpLogDatabaseName   string                     `toml:"mongo-oplog-database-name"`
	MongoOpLogCollectionName string                     `toml:"mongo-oplog-collection-name"`
	MongoCursorTimeout       string                     `toml:"mongo-cursor-timeout"`
	MongoDialSettings        mongo.MongoDialSettings    `toml:"mongo-dial-settings"`
	MongoSessionSettings     mongo.MongoSessionSettings `toml:"mongo-session-settings"`
	GtmSettings              g.GtmSettings              `toml:"gtm-settings"`
	Logs                     log.LogFiles               `toml:"logs"`
	ElasticUrls              types.Stringargs           `toml:"elasticsearch-urls"`
	ElasticUser              string                     `toml:"elasticsearch-user"`
	ElasticPassword          string                     `toml:"elasticsearch-password"`
	ElasticPemFile           string                     `toml:"elasticsearch-pem-file"`
	ElasticValidatePemFile   bool                       `toml:"elasticsearch-validate-pem-file"`
	ElasticVersion           string                     `toml:"elasticsearch-version"`
	ResumeName               string                     `toml:"resume-name"`
	NsRegex                  string                     `toml:"namespace-regex"`
	NsExcludeRegex           string                     `toml:"namespace-exclude-regex"`
	ClusterName              string                     `toml:"cluster-name"`
	Print                    bool                       `toml:"print-config"`
	Version                  bool
	Stats                    bool
	IndexStats               bool   `toml:"index-stats"`
	StatsDuration            string `toml:"stats-duration"`
	StatsIndexFormat         string `toml:"stats-index-format"`
	Gzip                     bool
	Verbose                  bool
	Resume                   bool
	ResumeWriteUnsafe        bool  `toml:"resume-write-unsafe"`
	ResumeFromTimestamp      int64 `toml:"resume-from-timestamp"`
	Replay                   bool
	DroppedDatabases         bool   `toml:"dropped-databases"`
	DroppedCollections       bool   `toml:"dropped-collections"`
	IndexFiles               bool   `toml:"index-files"`
	FileHighlighting         bool   `toml:"file-highlighting"`
	EnablePatches            bool   `toml:"enable-patches"`
	FailFast                 bool   `toml:"fail-fast"`
	IndexOplogTime           bool   `toml:"index-oplog-time"`
	ExitAfterDirectReads     bool   `toml:"exit-after-direct-reads"`
	MergePatchAttr           string `toml:"merge-patch-attribute"`
	ElasticMaxConns          int    `toml:"elasticsearch-max-conns"`
	ElasticRetry             bool   `toml:"elasticsearch-retry"`
	ElasticMaxDocs           int    `toml:"elasticsearch-max-docs"`
	ElasticMaxBytes          int    `toml:"elasticsearch-max-bytes"`
	ElasticMaxSeconds        int    `toml:"elasticsearch-max-seconds"`
	ElasticClientTimeout     int    `toml:"elasticsearch-client-timeout"`
	ElasticMajorVersion      int
	MaxFileSize              int64 `toml:"max-file-size"`
	ConfigFile               string
	Script                   []javascript
	Mapping                  []index.IndexTypeMapping
	FileNamespaces           types.Stringargs `toml:"file-namespaces"`
	PatchNamespaces          types.Stringargs `toml:"patch-namespaces"`
	Workers                  types.Stringargs
	Worker                   string
	DirectReadNs             types.Stringargs `toml:"direct-read-namespaces"`
	DirectReadLimit          int              `toml:"direct-read-limit"`
	DirectReadBatchSize      int              `toml:"direct-read-batch-size"`
	DirectReadersPerCol      int              `toml:"direct-readers-per-col"`
	MapperPluginPath         string           `toml:"mapper-plugin-path"`
	EnableHTTPServer         bool             `toml:"enable-http-server"`
	HTTPServerAddr           string           `toml:"http-server-addr"`
}
