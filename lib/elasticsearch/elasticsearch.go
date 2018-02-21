package elasticsearch

import (
	"context"
	"strings"

	"github.com/AlexsJones/monstache/lib/configuration"
	"github.com/AlexsJones/monstache/lib/index"
	"github.com/AlexsJones/monstache/lib/utils"
	"github.com/rwynn/elastic"
)

func DeleteIndexes(client *elastic.Client, db string, config *configuration.ConfigOptions) (err error) {
	ctx := context.Background()
	for ns, m := range index.MapIndexTypes {
		parts := strings.SplitN(ns, ".", 2)
		if parts[0] == db {
			if _, err = client.DeleteIndex(m.Index + "*").Do(ctx); err != nil {
				return
			}
		}
	}
	_, err = client.DeleteIndex(utils.NormalizeIndexName(db) + "*").Do(ctx)
	return
}

func DeleteIndex(client *elastic.Client, namespace string, config *configuration.ConfigOptions) (err error) {
	ctx := context.Background()
	esIndex := utils.NormalizeIndexName(namespace)
	if m := index.MapIndexTypes[namespace]; m != nil {
		esIndex = m.Index
	}
	_, err = client.DeleteIndex(esIndex).Do(ctx)
	return err
}

func EnsureFileMapping(client *elastic.Client, namespace string, config *configuration.ConfigOptions) (err error) {
	if config.ElasticMajorVersion < 5 {
		return EnsureFileMappingMapperAttachment(client, namespace, config)
	}
	return EnsureFileMappingIngestAttachment(client, namespace, config)
}

func EnsureFileMappingIngestAttachment(client *elastic.Client, namespace string, config *configuration.ConfigOptions) (err error) {
	ctx := context.Background()
	pipeline := map[string]interface{}{
		"description": "Extract file information",
		"processors": [1]map[string]interface{}{
			{
				"attachment": map[string]interface{}{
					"field": "file",
				},
			},
		},
	}
	_, err = client.IngestPutPipeline("attachment").BodyJson(pipeline).Do(ctx)
	return err
}

func EnsureFileMappingMapperAttachment(conn *elastic.Client, namespace string, config *configuration.ConfigOptions) (err error) {
	ctx := context.Background()
	parts := strings.SplitN(namespace, ".", 2)
	esIndex, esType := utils.NormalizeIndexName(namespace), utils.NormalizeTypeName(parts[1])
	if m := index.MapIndexTypes[namespace]; m != nil {
		esIndex, esType = m.Index, m.Type
	}
	props := map[string]interface{}{
		"properties": map[string]interface{}{
			"file": map[string]interface{}{
				"type": "attachment",
			},
		},
	}
	file := props["properties"].(map[string]interface{})["file"].(map[string]interface{})
	types := map[string]interface{}{
		esType: props,
	}
	mappings := map[string]interface{}{
		"mappings": types,
	}
	if config.FileHighlighting {
		file["fields"] = map[string]interface{}{
			"content": map[string]interface{}{
				"type":        "string",
				"term_vector": "with_positions_offsets",
				"store":       true,
			},
		}
	}
	var exists bool
	if exists, err = conn.IndexExists(esIndex).Do(ctx); exists {
		_, err = conn.PutMapping().Index(esIndex).Type(esType).BodyJson(types).Do(ctx)
	} else {
		_, err = conn.CreateIndex(esIndex).BodyJson(mappings).Do(ctx)
	}
	return err
}
