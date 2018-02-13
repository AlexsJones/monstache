package types

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
	"github.com/rwynn/gtm"
)

type gtmSettings struct {
	ChannelSize    int    `toml:"channel-size"`
	BufferSize     int    `toml:"buffer-size"`
	BufferDuration string `toml:"buffer-duration"`
}

func defaultIndexTypeMapping(op *gtm.Op) *IndexTypeMapping {
	return &IndexTypeMapping{
		Namespace: op.Namespace,
		Index:     normalizeIndexName(op.Namespace),
		Type:      normalizeTypeName(op.GetCollection()),
	}
}

func mapIndexType(op *gtm.Op) *IndexTypeMapping {
	mapping := defaultIndexTypeMapping(op)
	if mapIndexTypes != nil {
		if m := mapIndexTypes[op.Namespace]; m != nil {
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
		opIDStr = normalizeEsID(fmt.Sprintf("%v", op.Id))
	}
	return opIDStr
}

func gtmDefaultSettings() gtmSettings {
	return gtmSettings{
		ChannelSize:    gtmChannelSizeDefault,
		BufferSize:     32,
		BufferDuration: "750ms",
	}
}
