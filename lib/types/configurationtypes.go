package types

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"plugin"
	"regexp"

	"strconv"
	"strings"
	"time"

	"github.com/AlexsJones/monstache/lib/log"
	"github.com/globalsign/mgo"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"github.com/rwynn/monstache/monstachemap"

	elastic "gopkg.in/olivere/elastic.v5"
)

const mongoURLDefault string = "localhost"
const resumeNameDefault string = "default"
const elasticMaxConnsDefault int = 10
const elasticClientTimeoutDefault int = 60
const elasticMaxDocsDefault int = 1000
const gtmChannelSizeDefault int = 512

var mapEnvs map[string]*executionEnv
var mapperPlugin func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error)

var mapIndexTypes map[string]*IndexTypeMapping

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
	MongoURL                 string               `toml:"mongo-url"`
	MongoConfigURL           string               `toml:"mongo-config-url"`
	MongoPemFile             string               `toml:"mongo-pem-file"`
	MongoValidatePemFile     bool                 `toml:"mongo-validate-pem-file"`
	MongoOpLogDatabaseName   string               `toml:"mongo-oplog-database-name"`
	MongoOpLogCollectionName string               `toml:"mongo-oplog-collection-name"`
	MongoCursorTimeout       string               `toml:"mongo-cursor-timeout"`
	MongoDialSettings        mongoDialSettings    `toml:"mongo-dial-settings"`
	MongoSessionSettings     mongoSessionSettings `toml:"mongo-session-settings"`
	GtmSettings              gtmSettings          `toml:"gtm-settings"`
	Logs                     logFiles             `toml:"logs"`
	ElasticUrls              stringargs           `toml:"elasticsearch-urls"`
	ElasticUser              string               `toml:"elasticsearch-user"`
	ElasticPassword          string               `toml:"elasticsearch-password"`
	ElasticPemFile           string               `toml:"elasticsearch-pem-file"`
	ElasticValidatePemFile   bool                 `toml:"elasticsearch-validate-pem-file"`
	ElasticVersion           string               `toml:"elasticsearch-version"`
	ResumeName               string               `toml:"resume-name"`
	NsRegex                  string               `toml:"namespace-regex"`
	NsExcludeRegex           string               `toml:"namespace-exclude-regex"`
	ClusterName              string               `toml:"cluster-name"`
	Print                    bool                 `toml:"print-config"`
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
	Mapping                  []IndexTypeMapping
	FileNamespaces           stringargs `toml:"file-namespaces"`
	PatchNamespaces          stringargs `toml:"patch-namespaces"`
	Workers                  stringargs
	Worker                   string
	DirectReadNs             stringargs `toml:"direct-read-namespaces"`
	DirectReadLimit          int        `toml:"direct-read-limit"`
	DirectReadBatchSize      int        `toml:"direct-read-batch-size"`
	DirectReadersPerCol      int        `toml:"direct-readers-per-col"`
	MapperPluginPath         string     `toml:"mapper-plugin-path"`
	EnableHTTPServer         bool       `toml:"enable-http-server"`
	HTTPServerAddr           string     `toml:"http-server-addr"`
}

// Functionality ----------------------------------------------------------
func (config *ConfigOptions) isSharded() bool {
	return config.MongoConfigURL != ""
}
func (config *ConfigOptions) parseElasticsearchVersion(number string) (err error) {
	if number == "" {
		err = errors.New("Elasticsearch version cannot be blank")
	} else {
		versionParts := strings.Split(number, ".")
		var majorVersion int
		majorVersion, err = strconv.Atoi(versionParts[0])
		if err == nil {
			config.ElasticMajorVersion = majorVersion
			if majorVersion == 0 {
				err = errors.New("Invalid Elasticsearch major version 0")
			}
		}
	}
	return
}

func afterBulk(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		log.GetInstance().ErrorLog.Printf("Bulk index request with execution ID %d failed: %s", executionId, err)
	}
	if response != nil && response.Errors {
		failed := response.Failed()
		if failed != nil {
			log.GetInstance().ErrorLog.Printf("Bulk index request with execution ID %d has %d line failure(s)", executionId, len(failed))
			for i, item := range failed {
				json, err := json.Marshal(item)
				if err != nil {
					log.GetInstance().ErrorLog.Printf("Unable to marshall failed request line #%d: %s", i, err)
				} else {
					log.GetInstance().ErrorLog.Printf("Failed request line #%d details: %s", i, string(json))
				}
			}
		}
	}
}

func (config *ConfigOptions) newBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache")
	bulkService.Workers(config.ElasticMaxConns)
	bulkService.Stats(config.Stats)
	if config.ElasticMaxDocs != 0 {
		bulkService.BulkActions(config.ElasticMaxDocs)
	}
	if config.ElasticMaxBytes != 0 {
		bulkService.BulkSize(config.ElasticMaxBytes)
	}
	if config.ElasticRetry == false {
		bulkService.Backoff(&elastic.StopBackoff{})
	}
	bulkService.After(afterBulk)
	bulkService.FlushInterval(time.Duration(config.ElasticMaxSeconds) * time.Second)
	return bulkService.Do(context.Background())
}

func (config *ConfigOptions) newStatsBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache-stats")
	bulkService.Workers(1)
	bulkService.Stats(false)
	bulkService.BulkActions(1)
	bulkService.After(afterBulk)
	return bulkService.Do(context.Background())
}

func (config *ConfigOptions) needsSecureScheme() bool {
	if len(config.ElasticUrls) > 0 {
		for _, url := range config.ElasticUrls {
			if strings.HasPrefix(url, "https") {
				return true
			}
		}
	}
	return false

}

func (config *ConfigOptions) newElasticClient() (client *elastic.Client, err error) {
	var clientOptions []elastic.ClientOptionFunc
	var httpClient *http.Client
	clientOptions = append(clientOptions, elastic.SetErrorLog(log.GetInstance().ErrorLog))
	clientOptions = append(clientOptions, elastic.SetSniff(false))
	if config.needsSecureScheme() {
		clientOptions = append(clientOptions, elastic.SetScheme("https"))
	}
	if len(config.ElasticUrls) > 0 {
		clientOptions = append(clientOptions, elastic.SetURL(config.ElasticUrls...))
	} else {
		config.ElasticUrls = append(config.ElasticUrls, elastic.DefaultURL)
	}
	if config.Verbose {
		clientOptions = append(clientOptions, elastic.SetTraceLog(log.GetInstance().TraceLog))
	}
	if config.Gzip {
		clientOptions = append(clientOptions, elastic.SetGzip(true))
	}
	if config.ElasticUser != "" {
		clientOptions = append(clientOptions, elastic.SetBasicAuth(config.ElasticUser, config.ElasticPassword))
	}
	if config.ElasticRetry {
		d1, d2 := time.Duration(50)*time.Millisecond, time.Duration(20)*time.Second
		retrier := elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(d1, d2))
		clientOptions = append(clientOptions, elastic.SetRetrier(retrier))
	}
	httpClient, err = config.NewHTTPClient()
	if err != nil {
		return client, err
	}
	clientOptions = append(clientOptions, elastic.SetHttpClient(httpClient))
	return elastic.NewClient(clientOptions...)
}

func (config *ConfigOptions) NewHTTPClient() (client *http.Client, err error) {
	tlsConfig := &tls.Config{}
	if config.ElasticPemFile != "" {
		var ca []byte
		certs := x509.NewCertPool()
		if ca, err = ioutil.ReadFile(config.ElasticPemFile); err == nil {
			certs.AppendCertsFromPEM(ca)
			tlsConfig.RootCAs = certs
		} else {
			return client, err
		}
	}
	if config.ElasticValidatePemFile == false {
		// Turn off validation
		tlsConfig.InsecureSkipVerify = true
	}
	transport := &http.Transport{
		TLSHandshakeTimeout: time.Duration(30) * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	client = &http.Client{
		Timeout:   time.Duration(config.ElasticClientTimeout) * time.Second,
		Transport: transport,
	}
	return client, err
}

func (config *ConfigOptions) testElasticsearchConn(client *elastic.Client) (err error) {
	var number string
	url := config.ElasticUrls[0]
	number, err = client.ElasticsearchVersion(url)
	if err == nil {
		log.GetInstance().InfoLog.Printf("Successfully connected to Elasticsearch version %s", number)
		err = config.parseElasticsearchVersion(number)
	}
	return
}

func notifySdFailed(config *ConfigOptions, err error) {
	if err != nil {
		log.GetInstance().ErrorLog.Printf("Systemd notification failed: %s", err)
	} else {
		if config.Verbose {
			log.GetInstance().InfoLog.Println("Systemd notification not supported (i.e. NOTIFY_SOCKET is unset)")
		}
	}
}

func watchdogSdFailed(config *ConfigOptions, err error) {
	if err != nil {
		log.GetInstance().ErrorLog.Printf("Error determining systemd WATCHDOG interval: %s", err)
	} else {
		if config.Verbose {
			log.GetInstance().InfoLog.Println("Systemd WATCHDOG not enabled")
		}
	}
}

func (config *ConfigOptions) setDefaults() *ConfigOptions {
	if config.MongoURL == "" {
		config.MongoURL = mongoURLDefault
	}
	if config.ClusterName != "" {
		if config.ClusterName != "" && config.Worker != "" {
			config.ResumeName = fmt.Sprintf("%s:%s", config.ClusterName, config.Worker)
		} else {
			config.ResumeName = config.ClusterName
		}
		config.Resume = true
	} else if config.ResumeName == "" {
		if config.Worker != "" {
			config.ResumeName = config.Worker
		} else {
			config.ResumeName = resumeNameDefault
		}
	}
	if config.ElasticMaxConns == 0 {
		config.ElasticMaxConns = elasticMaxConnsDefault
	}
	if config.ElasticClientTimeout == 0 {
		config.ElasticClientTimeout = elasticClientTimeoutDefault
	}
	if config.MergePatchAttr == "" {
		config.MergePatchAttr = "json-merge-patches"
	}
	if config.ElasticMaxSeconds == 0 {
		config.ElasticMaxSeconds = 1
	}
	if config.ElasticMaxDocs == 0 {
		config.ElasticMaxDocs = elasticMaxDocsDefault
	}
	if config.MongoURL != "" {
		config.MongoURL = config.parseMongoURL(config.MongoURL)
	}
	if config.MongoConfigURL != "" {
		config.MongoConfigURL = config.parseMongoURL(config.MongoConfigURL)
	}
	if config.HTTPServerAddr == "" {
		config.HTTPServerAddr = ":8080"
	}
	if config.StatsIndexFormat == "" {
		config.StatsIndexFormat = "monstache.stats.2006-01-02"
	}
	return config
}

func (config *ConfigOptions) getAuthURL(inURL string) string {
	cred := strings.SplitN(config.MongoURL, "@", 2)
	if len(cred) == 2 {
		return cred[0] + "@" + inURL
	} else {
		return inURL
	}
}

func (config *ConfigOptions) configureMongo(session *mgo.Session) {
	session.SetMode(mgo.Primary, true)
	if config.MongoSessionSettings.SocketTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SocketTimeout) * time.Second
		session.SetSocketTimeout(timeOut)
	}
	if config.MongoSessionSettings.SyncTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SyncTimeout) * time.Second
		session.SetSyncTimeout(timeOut)
	}
}

func (config *ConfigOptions) dialMongo(inURL string) (*mgo.Session, error) {
	ssl := config.MongoDialSettings.Ssl || config.MongoPemFile != ""
	if ssl {
		tlsConfig := &tls.Config{}
		if config.MongoPemFile != "" {
			certs := x509.NewCertPool()
			if ca, err := ioutil.ReadFile(config.MongoPemFile); err == nil {
				certs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
			tlsConfig.RootCAs = certs
		}
		// Check to see if we don't need to validate the PEM
		if config.MongoValidatePemFile == false {
			// Turn off validation
			tlsConfig.InsecureSkipVerify = true
		}
		dialInfo, err := mgo.ParseURL(inURL)
		if err != nil {
			return nil, err
		}
		dialInfo.Timeout = time.Duration(10) * time.Second
		if config.MongoDialSettings.Timeout != -1 {
			dialInfo.Timeout = time.Duration(config.MongoDialSettings.Timeout) * time.Second
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			if err != nil {
				log.GetInstance().ErrorLog.Printf("Unable to dial mongodb: %s", err)
			}
			return conn, err
		}
		session, err := mgo.DialWithInfo(dialInfo)
		if err == nil {
			session.SetSyncTimeout(1 * time.Minute)
			session.SetSocketTimeout(1 * time.Minute)
		}
		return session, err
	}
	if config.MongoDialSettings.Timeout != -1 {
		return mgo.DialWithTimeout(inURL,
			time.Duration(config.MongoDialSettings.Timeout)*time.Second)
	}
	return mgo.Dial(inURL)
}

/*
if ssl=true is set on the connection string, remove the option
from the connection string and enable TLS because the mgo
driver does not support the option in the connection string
*/
func (config *ConfigOptions) parseMongoURL(inURL string) (outURL string) {
	const queryDelim string = "?"
	outURL = inURL
	hostQuery := strings.SplitN(outURL, queryDelim, 2)
	if len(hostQuery) == 2 {
		host, query := hostQuery[0], hostQuery[1]
		r := regexp.MustCompile(`ssl=true&?|&ssl=true$`)
		qstr := r.ReplaceAllString(query, "")
		if qstr != query {
			config.MongoDialSettings.Ssl = true
			if qstr == "" {
				outURL = host
			} else {
				outURL = strings.Join([]string{host, qstr}, queryDelim)
			}
		}
	}
	return
}

func (config *ConfigOptions) parseCommandLineFlags() *ConfigOptions {
	flag.BoolVar(&config.Print, "print-config", false, "Print the configuration and then exit")
	flag.StringVar(&config.MongoURL, "mongo-url", "", "MongoDB server or router server connection URL")
	flag.StringVar(&config.MongoConfigURL, "mongo-config-url", "", "MongoDB config server connection URL")
	flag.StringVar(&config.MongoPemFile, "mongo-pem-file", "", "Path to a PEM file for secure connections to MongoDB")
	flag.BoolVar(&config.MongoValidatePemFile, "mongo-validate-pem-file", true, "Set to boolean false to not validate the MongoDB PEM file")
	flag.StringVar(&config.MongoOpLogDatabaseName, "mongo-oplog-database-name", "", "Override the database name which contains the mongodb oplog")
	flag.StringVar(&config.MongoOpLogCollectionName, "mongo-oplog-collection-name", "", "Override the collection name which contains the mongodb oplog")
	flag.StringVar(&config.MongoCursorTimeout, "mongo-cursor-timeout", "", "Override the duration before a cursor timeout occurs when tailing the oplog")
	flag.StringVar(&config.ElasticVersion, "elasticsearch-version", "", "Specify elasticsearch version directly instead of getting it from the server")
	flag.StringVar(&config.ElasticUser, "elasticsearch-user", "", "The elasticsearch user name for basic auth")
	flag.StringVar(&config.ElasticPassword, "elasticsearch-password", "", "The elasticsearch password for basic auth")
	flag.StringVar(&config.ElasticPemFile, "elasticsearch-pem-file", "", "Path to a PEM file for secure connections to elasticsearch")
	flag.BoolVar(&config.ElasticValidatePemFile, "elasticsearch-validate-pem-file", true, "Set to boolean false to not validate the Elasticsearch PEM file")
	flag.IntVar(&config.ElasticMaxConns, "elasticsearch-max-conns", 0, "Elasticsearch max connections")
	flag.BoolVar(&config.ElasticRetry, "elasticsearch-retry", false, "True to retry failed request to Elasticsearch")
	flag.IntVar(&config.ElasticMaxDocs, "elasticsearch-max-docs", 0, "Number of docs to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxBytes, "elasticsearch-max-bytes", 0, "Number of bytes to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxSeconds, "elasticsearch-max-seconds", 0, "Number of seconds before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticClientTimeout, "elasticsearch-client-timeout", 0, "Number of seconds before a request to Elasticsearch is timed out")
	flag.Int64Var(&config.MaxFileSize, "max-file-size", 0, "GridFs file content exceeding this limit in bytes will not be indexed in Elasticsearch")
	flag.StringVar(&config.ConfigFile, "f", "", "Location of configuration file")
	flag.BoolVar(&config.DroppedDatabases, "dropped-databases", true, "True to delete indexes from dropped databases")
	flag.BoolVar(&config.DroppedCollections, "dropped-collections", true, "True to delete indexes from dropped collections")
	flag.BoolVar(&config.Version, "v", false, "True to print the version number")
	flag.BoolVar(&config.Gzip, "gzip", false, "True to use gzip for requests to elasticsearch")
	flag.BoolVar(&config.Verbose, "verbose", false, "True to output verbose messages")
	flag.BoolVar(&config.Stats, "stats", false, "True to print out statistics")
	flag.BoolVar(&config.IndexStats, "index-stats", false, "True to index stats in elasticsearch")
	flag.StringVar(&config.StatsDuration, "stats-duration", "", "The duration after which stats are logged")
	flag.StringVar(&config.StatsIndexFormat, "stats-index-format", "", "time.Time supported format to use for the stats index names")
	flag.BoolVar(&config.Resume, "resume", false, "True to capture the last timestamp of this run and resume on a subsequent run")
	flag.Int64Var(&config.ResumeFromTimestamp, "resume-from-timestamp", 0, "Timestamp to resume syncing from")
	flag.BoolVar(&config.ResumeWriteUnsafe, "resume-write-unsafe", false, "True to speedup writes of the last timestamp synched for resuming at the cost of error checking")
	flag.BoolVar(&config.Replay, "replay", false, "True to replay all events from the oplog and index them in elasticsearch")
	flag.BoolVar(&config.IndexFiles, "index-files", false, "True to index gridfs files into elasticsearch. Requires the elasticsearch mapper-attachments (deprecated) or ingest-attachment plugin")
	flag.BoolVar(&config.FileHighlighting, "file-highlighting", false, "True to enable the ability to highlight search times for a file query")
	flag.BoolVar(&config.EnablePatches, "enable-patches", false, "True to include an json-patch field on updates")
	flag.BoolVar(&config.FailFast, "fail-fast", false, "True to exit if a single _bulk request fails")
	flag.BoolVar(&config.IndexOplogTime, "index-oplog-time", false, "True to add date/time information from the oplog to each document when indexing")
	flag.BoolVar(&config.ExitAfterDirectReads, "exit-after-direct-reads", false, "True to exit the program after reading directly from the configured namespaces")
	flag.IntVar(&config.DirectReadLimit, "direct-read-limit", 0, "Maximum number of documents to fetch in each direct read query")
	flag.IntVar(&config.DirectReadBatchSize, "direct-read-batch-size", 0, "The batch size to set on direct read queries")
	flag.IntVar(&config.DirectReadersPerCol, "direct-readers-per-col", 0, "Number of goroutines directly reading a single collection")
	flag.StringVar(&config.MergePatchAttr, "merge-patch-attribute", "", "Attribute to store json-patch values under")
	flag.StringVar(&config.ResumeName, "resume-name", "", "Name under which to load/store the resume state. Defaults to 'default'")
	flag.StringVar(&config.ClusterName, "cluster-name", "", "Name of the monstache process cluster")
	flag.StringVar(&config.Worker, "worker", "", "The name of this worker in a multi-worker configuration")
	flag.StringVar(&config.MapperPluginPath, "mapper-plugin-path", "", "The path to a .so file to load as a document mapper plugin")
	flag.StringVar(&config.NsRegex, "namespace-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which match are synched to elasticsearch")
	flag.StringVar(&config.NsExcludeRegex, "namespace-exclude-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which do not match are synched to elasticsearch")
	flag.Var(&config.DirectReadNs, "direct-read-namespace", "A list of direct read namespaces")
	flag.Var(&config.ElasticUrls, "elasticsearch-url", "A list of Elasticsearch URLs")
	flag.Var(&config.FileNamespaces, "file-namespace", "A list of file namespaces")
	flag.Var(&config.PatchNamespaces, "patch-namespace", "A list of patch namespaces")
	flag.Var(&config.Workers, "workers", "A list of worker names")
	flag.BoolVar(&config.EnableHTTPServer, "enable-http-server", false, "True to enable an internal http server")
	flag.StringVar(&config.HTTPServerAddr, "http-server-addr", "", "The address the internal http server listens on")
	flag.Parse()
	return config
}

func (config *ConfigOptions) loadIndexTypes() {
	if config.Mapping != nil {
		mapIndexTypes = make(map[string]*IndexTypeMapping)
		for _, m := range config.Mapping {
			if m.Namespace != "" && m.Index != "" && m.Type != "" {
				mapIndexTypes[m.Namespace] = &IndexTypeMapping{
					Namespace: m.Namespace,
					Index:     normalizeIndexName(m.Index),
					Type:      normalizeTypeName(m.Type),
				}
			} else {
				panic("Mappings must specify namespace, index, and type attributes")
			}
		}
	}
}

func (config *ConfigOptions) loadScripts() {
	if config.Script != nil {
		mapEnvs = make(map[string]*executionEnv)
		for _, s := range config.Script {
			if s.Namespace != "" && s.Script != "" {
				env := &executionEnv{
					VM:      otto.New(),
					Script:  s.Script,
					Routing: s.Routing,
				}
				if err := env.VM.Set("module", make(map[string]interface{})); err != nil {
					panic(err)
				}
				if _, err := env.VM.Run(env.Script); err != nil {
					panic(err)
				}
				val, err := env.VM.Run("module.exports")
				if err != nil {
					panic(err)
				} else if !val.IsFunction() {
					panic("module.exports must be a function")

				}
				mapEnvs[s.Namespace] = env
			} else {
				panic("Scripts must specify namespace and script attributes")
			}
		}
	}
}

func normalizeIndexName(name string) (normal string) {
	normal = strings.ToLower(strings.TrimPrefix(name, "_"))
	return
}

func normalizeTypeName(name string) (normal string) {
	normal = strings.TrimPrefix(name, "_")
	return
}

func normalizeEsID(id string) (normal string) {
	normal = strings.TrimPrefix(id, "_")
	return
}

func (config *ConfigOptions) loadPlugins() *ConfigOptions {
	if config.MapperPluginPath != "" {
		p, err := plugin.Open(config.MapperPluginPath)
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to load mapper plugin %s: %s", config.MapperPluginPath, err)
		}
		mapper, err := p.Lookup("Map")
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to find symbol 'Map' in mapper plugin: %s", err)
		}
		switch mapper.(type) {
		case func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error):
			mapperPlugin = mapper.(func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error))
		default:
			log.GetInstance().ErrorLog.Panicf("Plugin 'Map' function must be typed %T", mapperPlugin)
		}
	}
	return config
}
