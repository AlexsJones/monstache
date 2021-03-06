// package main provides the monstache binary
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/AlexsJones/monstache/lib/configuration"
	"github.com/AlexsJones/monstache/lib/elasticsearch"
	"github.com/AlexsJones/monstache/lib/find"
	g "github.com/AlexsJones/monstache/lib/gmt"
	"github.com/AlexsJones/monstache/lib/index"
	"github.com/AlexsJones/monstache/lib/log"
	"github.com/AlexsJones/monstache/lib/mongo"
	"github.com/AlexsJones/monstache/lib/types"
	"github.com/coreos/go-systemd/daemon"
	"github.com/evanphx/json-patch"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"github.com/rwynn/elastic"
	"github.com/rwynn/gtm"
	"github.com/rwynn/gtm/consistent"
	"github.com/rwynn/monstache/monstachemap"
	"golang.org/x/net/context"
)

var gridByteBuffer bytes.Buffer

var chunksRegex = regexp.MustCompile("\\.chunks$")
var systemsRegex = regexp.MustCompile("system\\..+$")

const version = "3.6.1"

type httpServerCtx struct {
	httpServer *http.Server
	bulk       *elastic.BulkProcessor
	config     *configuration.ConfigOptions
	shutdown   bool
	started    time.Time
}

func convertSliceJavascript(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = convertMapJavascript(achild)
		case []interface{}:
			avc = convertSliceJavascript(achild)
		case bson.ObjectId:
			avc = achild.Hex()
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func convertMapJavascript(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = convertMapJavascript(child)
		case []interface{}:
			o[k] = convertSliceJavascript(child)
		case bson.ObjectId:
			o[k] = child.Hex()
		default:
			o[k] = v
		}
	}
	return o
}

func deepExportValue(a interface{}) (b interface{}) {
	switch t := a.(type) {
	case otto.Value:
		ex, err := t.Export()
		if err == nil {
			b = deepExportValue(ex)
		} else {
			log.GetInstance().ErrorLog.Printf("Error exporting from javascript: %s", err)
		}
	case map[string]interface{}:
		b = deepExportMap(t)
	case []interface{}:
		b = deepExportSlice(t)
	default:
		b = a
	}
	return
}

func deepExportSlice(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		avs = append(avs, deepExportValue(av))
	}
	return avs
}

func deepExportMap(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		o[k] = deepExportValue(v)
	}
	return o
}

func mapDataJavascript(op *gtm.Op) error {
	if configuration.MapEnvs == nil {
		return nil
	}
	if env := configuration.MapEnvs[op.Namespace]; env != nil {
		arg := convertMapJavascript(op.Data)
		val, err := env.VM.Call("module.exports", arg, arg)
		if err != nil {
			return err
		}
		if strings.ToLower(val.Class()) == "object" {
			data, err := val.Export()
			if err != nil {
				return err
			} else if data == val {
				return errors.New("Exported function must return an object")
			} else {
				dm := data.(map[string]interface{})
				op.Data = deepExportMap(dm)
			}
		} else {
			indexed, err := val.ToBoolean()
			if err != nil {
				return err
			} else if !indexed {
				op.Data = nil
			}
		}
	}
	return nil
}

func mapDataGolang(s *mgo.Session, op *gtm.Op) error {
	session := s.Copy()
	defer session.Close()
	input := &monstachemap.MapperPluginInput{
		Document:   op.Data,
		Namespace:  op.Namespace,
		Database:   op.GetDatabase(),
		Collection: op.GetCollection(),
		Operation:  op.Operation,
		Session:    session,
	}
	output, err := configuration.MapperPlugin(input)
	if err != nil {
		return err
	}
	if output != nil {
		if output.Drop {
			op.Data = nil
		} else {
			if output.Passthrough == false {
				op.Data = output.Document
			}
			var meta map[string]interface{}
			if output.Index != "" {
				meta["index"] = output.Index
			}
			if output.Type != "" {
				meta["type"] = output.Type
			}
			if output.Routing != "" {
				meta["routing"] = output.Routing
			}
			if output.Parent != "" {
				meta["parent"] = output.Parent
			}
			if output.Version != 0 {
				meta["version"] = output.Version
			}
			if output.VersionType != "" {
				meta["versionType"] = output.VersionType
			}
			if output.TTL != "" {
				meta["ttl"] = output.TTL
			}
			if output.Pipeline != "" {
				meta["pipeline"] = output.Pipeline
			}
			if output.RetryOnConflict != 0 {
				meta["retryOnConflict"] = output.RetryOnConflict
			}
			if len(meta) > 0 {
				op.Data["_meta_monstache"] = meta
			}
		}
	}
	return nil
}

func mapData(session *mgo.Session, config *configuration.ConfigOptions, op *gtm.Op) error {
	if config.MapperPluginPath != "" {
		return mapDataGolang(session, op)
	}
	return mapDataJavascript(op)
}

func prepareDataForIndexing(config *configuration.ConfigOptions, op *gtm.Op) {
	data := op.Data
	if config.IndexOplogTime {
		secs := int64(op.Timestamp >> 32)
		t := time.Unix(secs, 0).UTC()
		data["_oplog_ts"] = op.Timestamp
		data["_oplog_date"] = t.Format("2006/01/02 15:04:05")
	}
	delete(data, "_id")
	delete(data, "_type")
	delete(data, "_index")
	delete(data, "_score")
	delete(data, "_source")
	delete(data, "_meta_monstache")
}

func addFileContent(s *mgo.Session, op *gtm.Op, config *configuration.ConfigOptions) (err error) {
	session := s.Copy()
	defer session.Close()
	op.Data["file"] = ""
	gridByteBuffer.Reset()
	db, bucket :=
		session.DB(op.GetDatabase()),
		strings.SplitN(op.GetCollection(), ".", 2)[0]
	encoder := base64.NewEncoder(base64.StdEncoding, &gridByteBuffer)
	file, err := db.GridFS(bucket).OpenId(op.Id)
	if err != nil {
		return
	}
	defer file.Close()
	if config.MaxFileSize > 0 {
		if file.Size() > config.MaxFileSize {
			log.GetInstance().InfoLog.Printf("File %s md5(%s) exceeds max file size. file content omitted.",
				file.Name(), file.MD5())
			return
		}
	}
	if _, err = io.Copy(encoder, file); err != nil {
		return
	}
	if err = encoder.Close(); err != nil {
		return
	}
	op.Data["file"] = string(gridByteBuffer.Bytes())
	return
}

func notMonstache(op *gtm.Op) bool {
	return op.GetDatabase() != "monstache"
}

func notChunks(op *gtm.Op) bool {
	return !chunksRegex.MatchString(op.GetCollection())
}

func notConfig(op *gtm.Op) bool {
	return op.GetDatabase() != "config"
}

func notSystem(op *gtm.Op) bool {
	return !systemsRegex.MatchString(op.GetCollection())
}

func filterWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return validNameSpace.MatchString(op.Namespace)
	}
}

func filterInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return !invalidNameSpace.MatchString(op.Namespace)
	}
}

func ensureClusterTTL(session *mgo.Session) error {
	col := session.DB("monstache").C("cluster")
	return col.EnsureIndex(mgo.Index{
		Key:         []string{"expireAt"},
		Background:  true,
		ExpireAfter: time.Duration(30) * time.Second,
	})
}

func enableProcess(s *mgo.Session, config *configuration.ConfigOptions) (bool, error) {
	session := s.Copy()
	defer session.Close()
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	doc["_id"] = config.ResumeName
	doc["expireAt"] = time.Now().UTC()
	doc["pid"] = os.Getpid()
	if host, err := os.Hostname(); err == nil {
		doc["host"] = host
	} else {
		return false, err
	}
	err := col.Insert(doc)
	if err == nil {
		return true, nil
	}
	if mgo.IsDup(err) {
		return false, nil
	}
	return false, err
}

func resetClusterState(session *mgo.Session, config *configuration.ConfigOptions) error {
	col := session.DB("monstache").C("cluster")
	return col.RemoveId(config.ResumeName)
}

func ensureEnabled(s *mgo.Session, config *configuration.ConfigOptions) (enabled bool, err error) {
	session := s.Copy()
	defer session.Close()
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	if err = col.FindId(config.ResumeName).One(doc); err == nil {
		if doc["pid"] != nil && doc["host"] != nil {
			var hostname string
			pid := doc["pid"].(int)
			host := doc["host"].(string)
			if hostname, err = os.Hostname(); err == nil {
				enabled = (pid == os.Getpid() && host == hostname)
				if enabled {
					err = col.UpdateId(config.ResumeName,
						bson.M{"$set": bson.M{"expireAt": time.Now().UTC()}})
				}
			}
		}
	}
	return
}

func resumeWork(ctx *gtm.OpCtxMulti, session *mgo.Session, config *configuration.ConfigOptions) {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["ts"] != nil {
		ts := doc["ts"].(bson.MongoTimestamp)
		ctx.Since(ts)
	}
	ctx.Resume()
}

func saveTimestamp(s *mgo.Session, ts bson.MongoTimestamp, config *configuration.ConfigOptions) error {
	session := s.Copy()
	session.SetSocketTimeout(time.Duration(5) * time.Second)
	session.SetSyncTimeout(time.Duration(5) * time.Second)
	if config.ResumeWriteUnsafe {
		session.SetSafe(nil)
	}
	defer session.Close()
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	doc["ts"] = ts
	_, err := col.UpsertId(config.ResumeName, bson.M{"$set": doc})
	return err
}

func doDrop(mongo *mgo.Session, elastic *elastic.Client, op *gtm.Op, config *configuration.ConfigOptions) (err error) {
	if db, drop := op.IsDropDatabase(); drop {
		if config.DroppedDatabases {
			if err = elasticsearch.DeleteIndexes(elastic, db, config); err == nil {
				if e := dropDBMeta(mongo, db); e != nil {
					log.GetInstance().ErrorLog.Printf("Unable to delete metadata for db: %s", e)
				}
			}
		}
	} else if col, drop := op.IsDropCollection(); drop {
		if config.DroppedCollections {
			if err = elasticsearch.DeleteIndex(elastic, op.GetDatabase()+"."+col, config); err == nil {
				if e := dropCollectionMeta(mongo, op.GetDatabase()+"."+col); e != nil {
					log.GetInstance().ErrorLog.Printf("Unable to delete metadata for collection: %s", e)
				}
			}
		}
	}
	return
}

func doFileContent(mongo *mgo.Session, op *gtm.Op, config *configuration.ConfigOptions) (ingestAttachment bool, err error) {
	if !config.IndexFiles {
		return
	}
	if configuration.FileNamespaces[op.Namespace] {
		err = addFileContent(mongo, op, config)
		if config.ElasticMajorVersion >= 5 {
			if op.Data["file"] != "" {
				ingestAttachment = true
			}
		}
	}
	return
}

func addPatch(config *configuration.ConfigOptions, client *elastic.Client, op *gtm.Op,
	objectID string, indexType *index.IndexTypeMapping, meta *index.IndexingMeta) (err error) {
	var merges []interface{}
	var toJSON []byte
	if op.IsSourceDirect() {
		return nil
	}
	if op.Timestamp == 0 {
		return nil
	}
	if op.IsUpdate() {
		ctx := context.Background()
		service := client.Get()
		service.Id(objectID)
		service.Index(indexType.Index)
		service.Type(indexType.Type)
		if meta.Index != "" {
			service.Index(meta.Index)
		}
		if meta.Type != "" {
			service.Type(meta.Type)
		}
		if meta.Routing != "" {
			service.Routing(meta.Routing)
		}
		if meta.Parent != "" {
			service.Parent(meta.Parent)
		}
		var resp *elastic.GetResult
		if resp, err = service.Do(ctx); err == nil {
			if resp.Found {
				var src map[string]interface{}
				if err = json.Unmarshal(*resp.Source, &src); err == nil {
					if val, ok := src[config.MergePatchAttr]; ok {
						merges = val.([]interface{})
						for _, m := range merges {
							entry := m.(map[string]interface{})
							entry["ts"] = int(entry["ts"].(float64))
							entry["v"] = int(entry["v"].(float64))
						}
					}
					delete(src, config.MergePatchAttr)
					var fromJSON, mergeDoc []byte
					if fromJSON, err = json.Marshal(src); err == nil {
						if toJSON, err = json.Marshal(op.Data); err == nil {
							if mergeDoc, err = jsonpatch.CreateMergePatch(fromJSON, toJSON); err == nil {
								merge := make(map[string]interface{})
								merge["ts"] = op.Timestamp >> 32
								merge["p"] = string(mergeDoc)
								merge["v"] = len(merges) + 1
								merges = append(merges, merge)
								op.Data[config.MergePatchAttr] = merges
							}
						}
					}
				}
			} else {
				err = errors.New("Last document revision not found")
			}

		}
	} else {
		if _, found := op.Data[config.MergePatchAttr]; !found {
			if toJSON, err = json.Marshal(op.Data); err == nil {
				merge := make(map[string]interface{})
				merge["v"] = 1
				merge["ts"] = op.Timestamp >> 32
				merge["p"] = string(toJSON)
				merges = append(merges, merge)
				op.Data[config.MergePatchAttr] = merges
			}
		}
	}
	return
}

func doIndexing(config *configuration.ConfigOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op, ingestAttachment bool) (err error) {
	meta := g.ParseIndexMeta(op)
	prepareDataForIndexing(config, op)
	objectID, indexType := g.OpIDToString(op), g.MapIndexType(op)
	if config.EnablePatches {
		if configuration.PatchNamespaces[op.Namespace] {
			if e := addPatch(config, client, op, objectID, indexType, meta); e != nil {
				log.GetInstance().ErrorLog.Printf("Unable to save json-patch info: %s", e)
			}
		}
	}
	req := elastic.NewBulkIndexRequest()

	req.Id(objectID)
	req.Index(indexType.Index)
	req.Type(indexType.Type)
	req.Doc(op.Data)

	if meta.Index != "" {
		req.Index(meta.Index)
	}
	if meta.Type != "" {
		req.Type(meta.Type)
	}
	if meta.Routing != "" {
		req.Routing(meta.Routing)
	}
	if meta.Parent != "" {
		req.Parent(meta.Parent)
	}
	if meta.Version != 0 {
		req.Version(meta.Version)
	}
	if meta.VersionType != "" {
		req.VersionType(meta.VersionType)
	}
	if meta.TTL != "" {
		req.TTL(meta.TTL)
	}
	if meta.Pipeline != "" {
		req.Pipeline(meta.Pipeline)
	}
	if meta.RetryOnConflict != 0 {
		req.RetryOnConflict(meta.RetryOnConflict)
	}
	if ingestAttachment {
		req.Pipeline("attachment")
	}

	bulk.Add(req)
	if meta.ShouldSave() {
		if e := setIndexMeta(mongo, op.Namespace, objectID, meta); e != nil {
			log.GetInstance().ErrorLog.Printf("Unable to save routing info: %s", e)
		}
	}
	return
}

func doIndex(config *configuration.ConfigOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op, ingestAttachment bool) (err error) {
	if err = mapData(mongo, config, op); err == nil {
		if op.Data != nil {
			err = doIndexing(config, mongo, bulk, client, op, ingestAttachment)
		} else if op.IsUpdate() {
			doDelete(mongo, bulk, op)
		}
	}
	return
}

func doIndexStats(config *configuration.ConfigOptions, bulkStats *elastic.BulkProcessor, stats elastic.BulkProcessorStats) (err error) {
	var hostname string
	doc := make(map[string]interface{})
	t := time.Now().UTC()
	doc["Timestamp"] = t.Format("2006-01-02T15:04:05")
	hostname, err = os.Hostname()
	if err == nil {
		doc["Host"] = hostname
	}
	doc["Pid"] = os.Getpid()
	doc["Stats"] = stats
	index := t.Format(config.StatsIndexFormat)
	req := elastic.NewBulkIndexRequest().Index(index).Type("stats")
	req.Doc(doc)
	bulkStats.Add(req)
	return
}

func dropDBMeta(session *mgo.Session, db string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"db": db}
	_, err = col.RemoveAll(q)
	return
}

func dropCollectionMeta(session *mgo.Session, namespace string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"namespace": namespace}
	_, err = col.RemoveAll(q)
	return
}

func setIndexMeta(session *mgo.Session, namespace, id string, meta *index.IndexingMeta) error {
	col := session.DB("monstache").C("meta")
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	doc := make(map[string]interface{})
	doc["routing"] = meta.Routing
	doc["index"] = meta.Index
	doc["type"] = meta.Type
	doc["parent"] = meta.Parent
	doc["pipeline"] = meta.Pipeline
	doc["db"] = strings.SplitN(namespace, ".", 2)[0]
	doc["namespace"] = namespace
	_, err := col.UpsertId(metaID, bson.M{"$set": doc})
	return err
}

func getIndexMeta(session *mgo.Session, namespace, id string) (meta *index.IndexingMeta) {
	meta = &index.IndexingMeta{}
	col := session.DB("monstache").C("meta")
	doc := make(map[string]interface{})
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	col.FindId(metaID).One(doc)
	if doc["routing"] != nil {
		meta.Routing = doc["routing"].(string)
	}
	if doc["index"] != nil {
		meta.Index = doc["index"].(string)
	}
	if doc["type"] != nil {
		meta.Type = doc["type"].(string)
	}
	if doc["parent"] != nil {
		meta.Parent = doc["parent"].(string)
	}
	if doc["pipeline"] != nil {
		meta.Pipeline = doc["pipeline"].(string)
	}
	col.RemoveId(metaID)
	return
}

func loadBuiltinFunctions(s *mgo.Session) {
	if configuration.MapEnvs == nil {
		return
	}
	for ns, env := range configuration.MapEnvs {
		var fa *find.FindConf
		fa = &find.FindConf{
			Session: s,
			Name:    "findId",
			Vm:      env.VM,
			Ns:      ns,
			ById:    true,
		}
		if err := env.VM.Set(fa.Name, find.MakeFind(fa)); err != nil {
			panic(err)
		}
		fa = &find.FindConf{
			Session: s,
			Name:    "findOne",
			Vm:      env.VM,
			Ns:      ns,
		}
		if err := env.VM.Set(fa.Name, find.MakeFind(fa)); err != nil {
			panic(err)
		}
		fa = &find.FindConf{
			Session: s,
			Name:    "find",
			Vm:      env.VM,
			Ns:      ns,
			Multi:   true,
		}
		if err := env.VM.Set(fa.Name, find.MakeFind(fa)); err != nil {
			panic(err)
		}
	}
}

func doDelete(mongo *mgo.Session, bulk *elastic.BulkProcessor, op *gtm.Op) {
	objectID, indexType, meta := g.OpIDToString(op), g.MapIndexType(op), &index.IndexingMeta{}
	if configuration.MapEnvs != nil {
		if env := configuration.MapEnvs[op.Namespace]; env != nil && env.Routing {
			meta = getIndexMeta(mongo, op.Namespace, objectID)
		}
	}
	req := elastic.NewBulkDeleteRequest()
	req.Id(objectID)
	req.Index(indexType.Index)
	req.Type(indexType.Type)
	req.Version(int64(op.Timestamp))
	req.VersionType("external")
	if meta.Index != "" {
		req.Index(meta.Index)
	}
	if meta.Type != "" {
		req.Type(meta.Type)
	}
	if meta.Routing != "" {
		req.Routing(meta.Routing)
	}
	if meta.Parent != "" {
		req.Parent(meta.Parent)
	}
	bulk.Add(req)
	return
}

func (ctx *httpServerCtx) serveHttp() {
	s := ctx.httpServer
	if ctx.config.Verbose {
		log.GetInstance().InfoLog.Printf("Starting http server at %s", s.Addr)
	}
	ctx.started = time.Now()
	err := s.ListenAndServe()
	if !ctx.shutdown {
		log.GetInstance().ErrorLog.Panicf("Unable to serve http at address %s: %s", s.Addr, err)
	}
}

func (ctx *httpServerCtx) buildServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/started", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data := (time.Now().Sub(ctx.started)).String()
		w.Write([]byte(data))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	if ctx.config.Stats {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, req *http.Request) {
			stats, err := json.MarshalIndent(ctx.bulk.Stats(), "", "    ")
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(stats)
			} else {
				w.WriteHeader(500)
				fmt.Fprintf(w, "Unable to print statistics: %s", err)
			}
		})
	}
	mux.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
		conf, err := json.MarshalIndent(ctx.config, "", "    ")
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			w.Write(conf)
		} else {
			w.WriteHeader(500)
			fmt.Fprintf(w, "Unable to print config: %s", err)
		}
	})
	s := &http.Server{
		Addr:     ctx.config.HTTPServerAddr,
		Handler:  mux,
		ErrorLog: log.GetInstance().ErrorLog,
	}
	ctx.httpServer = s
}

func NotifySd(config *configuration.ConfigOptions) {
	var interval time.Duration
	if config.Verbose {
		log.GetInstance().InfoLog.Println("Sending systemd READY=1")
	}
	sent, err := daemon.SdNotify(false, "READY=1")
	if sent {
		if config.Verbose {
			log.GetInstance().InfoLog.Println("READY=1 successfully sent to systemd")
		}
	} else {
		configuration.NotifySdFailed(config, err)
		return
	}
	interval, err = daemon.SdWatchdogEnabled(false)
	if err != nil || interval == 0 {
		configuration.WatchdogSdFailed(config, err)
		return
	}
	for {
		if config.Verbose {
			log.GetInstance().InfoLog.Println("Sending systemd WATCHDOG=1")
		}
		sent, err = daemon.SdNotify(false, "WATCHDOG=1")
		if sent {
			if config.Verbose {
				log.GetInstance().InfoLog.Println("WATCHDOG=1 successfully sent to systemd")
			}
		} else {
			configuration.NotifySdFailed(config, err)
			return
		}
		time.Sleep(interval / 2)
	}
}

func main() {
	enabled := true
	config := &configuration.ConfigOptions{
		MongoDialSettings:    mongo.MongoDialSettings{Timeout: -1},
		MongoSessionSettings: mongo.MongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
		GtmSettings:          g.GtmDefaultSettings(),
	}
	config.ParseCommandLineFlags()
	if config.Version {
		fmt.Println(version)
		os.Exit(0)
	}
	config.LoadConfigFile().SetDefaults()
	if config.Print {
		config.Dump()
		os.Exit(0)
	}
	config.LoadPlugins()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	mongo, err := config.DialMongo(config.MongoURL)
	if err != nil {
		log.GetInstance().ErrorLog.Panicf("Unable to connect to mongodb using URL %s: %s", config.MongoURL, err)
	}
	if mongoInfo, err := mongo.BuildInfo(); err == nil {
		log.GetInstance().InfoLog.Printf("Successfully connected to MongoDB version %s", mongoInfo.Version)
	} else {
		log.GetInstance().InfoLog.Println("Successfully connected to MongoDB")
	}
	defer mongo.Close()
	config.ConfigureMongo(mongo)
	loadBuiltinFunctions(mongo)

	elasticClient, err := config.NewElasticClient()
	if err != nil {
		log.GetInstance().ErrorLog.Panicf("Unable to create elasticsearch client: %s", err)
	}
	if config.ElasticVersion == "" {
		if err := config.TestElasticsearchConn(elasticClient); err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to validate connection to elasticsearch using client %s: %s",
				elasticClient, err)
		}
	} else {
		if err := config.ParseElasticsearchVersion(config.ElasticVersion); err != nil {
			log.GetInstance().ErrorLog.Panicf("Elasticsearch version must conform to major.minor.fix: %s", err)
		}
	}
	bulk, err := config.NewBulkProcessor(elasticClient)
	if err != nil {
		log.GetInstance().ErrorLog.Panicf("Unable to start bulk processor: %s", err)
	}
	defer bulk.Stop()
	var bulkStats *elastic.BulkProcessor
	if config.IndexStats {
		bulkStats, err = config.NewStatsBulkProcessor(elasticClient)
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to start stats bulk processor: %s", err)
		}
		defer bulkStats.Stop()
	}

	go func() {
		<-sigs
		done <- true
	}()

	var after gtm.TimestampGenerator
	if config.Resume {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			ts := gtm.LastOpTimestamp(session, options)
			if config.Replay {
				ts = bson.MongoTimestamp(0)
			} else if config.ResumeFromTimestamp != 0 {
				ts = bson.MongoTimestamp(config.ResumeFromTimestamp)
			} else {
				collection := session.DB("monstache").C("monstache")
				doc := make(map[string]interface{})
				collection.FindId(config.ResumeName).One(doc)
				if doc["ts"] != nil {
					ts = doc["ts"].(bson.MongoTimestamp)
				}
			}
			return ts
		}
	} else if config.Replay {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return bson.MongoTimestamp(0)
		}
	}

	if config.IndexFiles {
		if len(config.FileNamespaces) == 0 {
			log.GetInstance().ErrorLog.Fatalln("File indexing is ON but no file namespaces are configured")
		}
		for _, namespace := range config.FileNamespaces {
			if err := elasticsearch.EnsureFileMapping(elasticClient, namespace, config); err != nil {
				panic(err)
			}
			if config.ElasticMajorVersion >= 5 {
				break
			}
		}
	}

	var nsFilter, filter, directReadFilter gtm.OpFilter
	filterChain := []gtm.OpFilter{notMonstache, notSystem, notChunks}
	if config.IsSharded() {
		filterChain = append(filterChain, notConfig)
	}
	if config.NsRegex != "" {
		filterChain = append(filterChain, filterWithRegex(config.NsRegex))
	}
	if config.NsExcludeRegex != "" {
		filterChain = append(filterChain, filterInverseWithRegex(config.NsExcludeRegex))
	}
	if config.Worker != "" {
		workerFilter, err := consistent.ConsistentHashFilter(config.Worker, config.Workers)
		if err != nil {
			panic(err)
		}
		filter = workerFilter
		directReadFilter = workerFilter
	} else if config.Workers != nil {
		panic("Workers configured but this worker is undefined. worker must be set to one of the workers.")
	}
	nsFilter = gtm.ChainOpFilters(filterChain...)
	var oplogDatabaseName, oplogCollectionName, cursorTimeout *string
	if config.MongoOpLogDatabaseName != "" {
		oplogDatabaseName = &config.MongoOpLogDatabaseName
	}
	if config.MongoOpLogCollectionName != "" {
		oplogCollectionName = &config.MongoOpLogCollectionName
	}
	if config.MongoCursorTimeout != "" {
		cursorTimeout = &config.MongoCursorTimeout
	}
	if config.ClusterName != "" {
		if err = ensureClusterTTL(mongo); err == nil {
			log.GetInstance().InfoLog.Printf("Joined cluster %s", config.ClusterName)
		} else {
			log.GetInstance().ErrorLog.Panicf("Unable to enable cluster mode: %s", err)
		}
		enabled, err = enableProcess(mongo, config)
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to determine enabled cluster process: %s", err)
		}
		if !enabled {
			config.DirectReadNs = types.Stringargs{}
		}
	}
	gtmBufferDuration, err := time.ParseDuration(config.GtmSettings.BufferDuration)
	if err != nil {
		log.GetInstance().ErrorLog.Panicf("Unable to parse gtm buffer duration %s: %s", config.GtmSettings.BufferDuration, err)
	}
	var mongos []*mgo.Session
	var configSession *mgo.Session
	if config.IsSharded() {
		// if we have a config server URL then we are running in a sharded cluster
		configSession, err = config.DialMongo(config.MongoConfigURL)
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to connect to mongodb config server using URL %s: %s", config.MongoConfigURL, err)
		}
		config.ConfigureMongo(configSession)
		// get the list of shard servers
		shardInfos := gtm.GetShards(configSession)
		if len(shardInfos) == 0 {
			log.GetInstance().ErrorLog.Fatalln("Shards enabled but none found in config.shards collection")
		}
		// add each shard server to the sync list
		for _, shardInfo := range shardInfos {
			log.GetInstance().InfoLog.Printf("Adding shard found at %s\n", shardInfo.GetURL())
			shardURL := config.GetAuthURL(shardInfo.GetURL())
			shard, err := config.DialMongo(shardURL)
			if err != nil {
				log.GetInstance().ErrorLog.Panicf("Unable to connect to mongodb shard using URL %s: %s", shardURL, err)
			}
			defer shard.Close()
			config.ConfigureMongo(shard)
			mongos = append(mongos, shard)
		}
	} else {
		mongos = append(mongos, mongo)
	}

	gtmOpts := &gtm.Options{
		After:               after,
		Filter:              filter,
		NamespaceFilter:     nsFilter,
		OpLogDatabaseName:   oplogDatabaseName,
		OpLogCollectionName: oplogCollectionName,
		CursorTimeout:       cursorTimeout,
		ChannelSize:         config.GtmSettings.ChannelSize,
		Ordering:            gtm.Oplog,
		WorkerCount:         1,
		BufferDuration:      gtmBufferDuration,
		BufferSize:          config.GtmSettings.BufferSize,
		DirectReadNs:        config.DirectReadNs,
		DirectReadLimit:     config.DirectReadLimit,
		DirectReadBatchSize: config.DirectReadBatchSize,
		DirectReadersPerCol: config.DirectReadersPerCol,
		DirectReadFilter:    directReadFilter,
	}

	gtmCtx := gtm.StartMulti(mongos, gtmOpts)

	if config.IsSharded() {

		gtmCtx.AddShardListener(configSession, gtmOpts, config.MakeShardInsertHandler())
	}
	if config.ClusterName != "" {
		if enabled {
			log.GetInstance().InfoLog.Printf("Starting work for cluster %s", config.ClusterName)
		} else {
			log.GetInstance().InfoLog.Printf("Pausing work for cluster %s", config.ClusterName)
			gtmCtx.Pause()
		}
	}
	timestampTicker := time.NewTicker(10 * time.Second)
	if config.Resume == false {
		timestampTicker.Stop()
	}
	heartBeat := time.NewTicker(10 * time.Second)
	if config.ClusterName == "" {
		heartBeat.Stop()
	}
	statsTimeout := time.Duration(30) * time.Second
	if config.StatsDuration != "" {
		statsTimeout, err = time.ParseDuration(config.StatsDuration)
		if err != nil {
			log.GetInstance().ErrorLog.Panicf("Unable to parse stats duration: %s", err)
		}
	}
	printStats := time.NewTicker(statsTimeout)
	if config.Stats == false {
		printStats.Stop()
	}
	exitStatus := 0
	if len(config.DirectReadNs) > 0 {
		go func(c *gtm.OpCtxMulti, config *configuration.ConfigOptions) {
			c.DirectReadWg.Wait()
			if config.ExitAfterDirectReads {
				done <- true
			}
		}(gtmCtx, config)
	}
	go NotifySd(config)
	var hsc *httpServerCtx
	if config.EnableHTTPServer {
		hsc = &httpServerCtx{
			bulk:   bulk,
			config: config,
		}
		hsc.buildServer()
		go hsc.serveHttp()
	}
	if config.Verbose {
		log.GetInstance().InfoLog.Println("Entering event loop")
	}
	var lastTimestamp, lastSavedTimestamp bson.MongoTimestamp
	for {
		select {
		case <-done:
			if hsc != nil {
				hsc.shutdown = true
				hsc.httpServer.Shutdown(context.Background())
			}
			bulk.Flush()
			bulk.Stop()
			if bulkStats != nil {
				bulkStats.Flush()
				bulkStats.Stop()
			}
			if config.ClusterName != "" {
				resetClusterState(mongo, config)
			}
			mongo.Close()
			os.Exit(exitStatus)
		case <-timestampTicker.C:
			if lastTimestamp > lastSavedTimestamp {
				bulk.Flush()
				if saveTimestamp(mongo, lastTimestamp, config); err == nil {
					lastSavedTimestamp = lastTimestamp
				} else {
					gtmCtx.ErrC <- err
				}
			}
		case <-heartBeat.C:
			if config.ClusterName == "" {
				break
			}
			if enabled {
				enabled, err = ensureEnabled(mongo, config)
				if !enabled {
					log.GetInstance().InfoLog.Printf("Pausing work for cluster %s", config.ClusterName)
					gtmCtx.Pause()
					bulk.Stop()
				}
			} else {
				enabled, err = enableProcess(mongo, config)
				if enabled {
					log.GetInstance().InfoLog.Printf("Resuming work for cluster %s", config.ClusterName)
					bulk.Start(context.Background())
					resumeWork(gtmCtx, mongo, config)
				}
			}
			if err != nil {
				gtmCtx.ErrC <- err
			}
		case <-printStats.C:
			if !enabled {
				break
			}
			if config.IndexStats {
				if err := doIndexStats(config, bulkStats, bulk.Stats()); err != nil {
					log.GetInstance().ErrorLog.Printf("Error indexing statistics: %s", err)
				}
			} else {
				stats, err := json.Marshal(bulk.Stats())
				if err != nil {
					log.GetInstance().ErrorLog.Printf("Unable to log statistics: %s", err)
				} else {
					log.GetInstance().StatsLog.Println(string(stats))
				}
			}
		case err = <-gtmCtx.ErrC:
			exitStatus = 1
			log.GetInstance().ErrorLog.Println(err)
			if config.FailFast {
				os.Exit(exitStatus)
			}
		case op := <-gtmCtx.OpC:
			if !enabled {
				break
			}
			if op.IsSourceOplog() {
				lastTimestamp = op.Timestamp
			}
			if op.IsDrop() {
				bulk.Flush()
				if err = doDrop(mongo, elasticClient, op, config); err != nil {
					gtmCtx.ErrC <- err
				}
			} else if op.IsDelete() {
				doDelete(mongo, bulk, op)
			} else if op.Data != nil {
				ingestAttachment := false
				if ingestAttachment, err = doFileContent(mongo, op, config); err != nil {
					gtmCtx.ErrC <- err
				}
				if err = doIndex(config, mongo, bulk, elasticClient, op, ingestAttachment); err != nil {
					gtmCtx.ErrC <- err
				}
			}
		}
	}
}
