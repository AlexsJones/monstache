package g

import (
	"fmt"

	"github.com/AlexsJones/monstache/lib/defaults"
	"github.com/AlexsJones/monstache/lib/index"
	"github.com/AlexsJones/monstache/lib/log"
	"github.com/AlexsJones/monstache/lib/utils"
	"github.com/globalsign/mgo/bson"
	"github.com/robertkrimen/otto"
	"github.com/rwynn/gtm"
)

func defaultIndexTypeMapping(op *gtm.Op) *index.IndexTypeMapping {
	return &index.IndexTypeMapping{
		Namespace: op.Namespace,
		Index:     utils.NormalizeIndexName(op.Namespace),
		Type:      utils.NormalizeTypeName(op.GetCollection()),
	}
}

func mapIndexType(op *gtm.Op) *index.IndexTypeMapping {
	mapping := defaultIndexTypeMapping(op)
	if index.MapIndexTypes != nil {
		if m := index.MapIndexTypes[op.Namespace]; m != nil {
			mapping = m
		}
	}
	return mapping
}

func opIDToString(op *gtm.Op) string {
	var opIDStr string
	switch op.Id.(type) {
	case bson.ObjectId:
		opIDStr = op.Id.(bson.ObjectId).Hex()
	case float64:
		intID := int(op.Id.(float64))
		if op.Id.(float64) == float64(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	case float32:
		intID := int(op.Id.(float32))
		if op.Id.(float32) == float32(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	default:
		opIDStr = utils.NormalizeEsID(fmt.Sprintf("%v", op.Id))
	}
	return opIDStr
}

func gtmDefaultSettings() GtmSettings {
	return GtmSettings{
		ChannelSize:    defaults.GtmChannelSizeDefault,
		BufferSize:     32,
		BufferDuration: "750ms",
	}
}

func parseIndexMeta(op *gtm.Op) (meta *index.IndexingMeta) {
	meta = &index.IndexingMeta{
		Version:     int64(op.Timestamp),
		VersionType: "external",
	}
	if m, ok := op.Data["_meta_monstache"]; ok {
		switch m.(type) {
		case map[string]interface{}:
			metaAttrs := m.(map[string]interface{})
			meta.Load(metaAttrs)
		case otto.Value:
			ex, err := m.(otto.Value).Export()
			if err == nil && ex != m {
				switch ex.(type) {
				case map[string]interface{}:
					metaAttrs := ex.(map[string]interface{})
					meta.Load(metaAttrs)
				default:
					log.GetInstance().ErrorLog.Println("Invalid indexing metadata")
				}
			}
		default:
			log.GetInstance().ErrorLog.Println("Invalid indexing metadata")
		}
	}
	return meta
}
