/*
Copyright (C) 2017 Verizon. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ekk

import (
	"context"
	"fmt"
	"time"

	"github.com/verizonlabs/northstar/pkg/ekk/config"
	"github.com/verizonlabs/northstar/pkg/mlog"

	"strings"

	"net/http"

	elastic "gopkg.in/olivere/elastic.v5"
)

type (
	Elasticsearch struct {
		client *elastic.Client
	}

	Log struct {
		HostIP        string `json:"host_ip"`
		Marker        string `json:"marker"`
		Version       string `json:"version"`
		Level         string `json:"level"`
		MesosTaskID   string `json:"mesos_task_id"`
		ProcessID     string `json:"process_id"`
		Group         string `json:"group"`
		App           string `json:"app"`
		ProcessName   string `json:"process_name"`
		CorrelationID string `json:"correlation_id"`
		File          string `json:"file"`
		Time          string `json:"time"`
		Message       string `json:"message"`
	}
)

// Log field index
const (
	HostIP = iota
	Marker
	Version
	Level
	MesosTaskID
	ProcessID
	Group
	App
	ProcessName
	CorrelationID
	File
	Time
	Message
)

// Elasticsearch data mapping
const template = `{
	"order": 0,
	"template": "logging-*",
	"settings":{
		"number_of_shards": %d,
		"number_of_replicas": %d
	},
	"mappings": {
		"log": {
			"properties": {
				"time": {
					"type": "date",
					"format": "yyyy/MM/dd HH:mm:ss.SSSSSSSSS"
				}
			}
		}
	},
	"aliases": {
    "logging": {}
  }
}`

// NewElasticsearch returns an Elasticsearch instance.
func NewElasticsearch() (*Elasticsearch, error) {
	client, err := elastic.NewClient(elastic.SetURL(strings.Split(config.ElasticsearchURLs, ",")...))
	if err != nil {
		return nil, err
	}
	e := &Elasticsearch{
		client: client,
	}
	err = e.createIndexTemplate()
	if err != nil {
		return nil, err
	}
	go e.purgeIndices()
	return e, nil
}

// createIndexTemplate creates or updates elasticsearch logging index template.
func (e *Elasticsearch) createIndexTemplate() (err error) {
	mlog.Info("create logging index template")
	ctx := context.Background()
	exists, err := e.client.IndexTemplateExists("log").Do(ctx)
	if err != nil {
		return
	}
	if !exists {
		_, err = e.client.IndexPutTemplate("log").
			BodyString(fmt.Sprintf(template, config.ElasticsearchShards, config.ElasticsearchReplicas)).
			Do(ctx)
	}
	return
}

// WriteLog writes to into daily Elasticsearch index.
func (e *Elasticsearch) WriteLog(msg []byte) (err error) {
	index := "logging-" + time.Now().Format("2006.01.02")
	mlog.Debug("write log")
	log := decodeLog(msg)
	if log != nil {
		ctx := context.Background()
		_, err = e.client.Index().
			Index(index).
			Type("log").
			BodyJson(log).
			Do(ctx)
	}
	return
}

func (e *Elasticsearch) purgeIndices() {
	for range time.Tick(1 * time.Minute) {
		// Remove old indices
		mlog.Info("purge old indices")
		indexNames, err := e.client.IndexNames()
		if err != nil {
			mlog.Error("error fetching indices")
		}
		ttlDate := time.Now().Add(-24 * time.Hour * time.Duration(config.ElasticsearchTTL)).Format("2006.01.02")
		oldIndices := []string{}
		for _, i := range indexNames {
			if strings.HasPrefix(i, "logging-") {
				if strings.Split(i, "-")[1] <= ttlDate {
					oldIndices = append(oldIndices, i)
				}
			}
		}
		ctx := context.Background()
		if _, err := e.client.DeleteIndex(oldIndices...).Do(ctx); err != nil {
			mlog.Error("error deleting old indices=%v", oldIndices)
		}

		// Remove old indices from alias
		_, err = e.client.Alias().
			Action(elastic.NewAliasRemoveAction("logging").
				Index(oldIndices...)).
			Do(ctx)
		if err != nil {
			if err, ok := err.(*elastic.Error); ok {
				if err.Status == http.StatusNotFound {
					continue
				}
			}
			mlog.Error("error purging indices from alias, index=%s, error=%v", err)
		}
	}
}

func decodeLog(msg []byte) *Log {
	// TODO: Handle third-party logs
	fields := strings.Split(string(msg), "|")
	if len(fields) > Message {
		return &Log{
			HostIP:        fields[HostIP],
			Marker:        fields[Marker],
			Version:       fields[Version],
			Level:         fields[Level],
			MesosTaskID:   fields[MesosTaskID],
			ProcessID:     fields[ProcessID],
			Group:         fields[Group],
			App:           fields[App],
			ProcessName:   fields[ProcessName],
			CorrelationID: fields[CorrelationID],
			File:          fields[File],
			Time:          fields[Time],
			Message:       fields[Message],
		}
	}
	return nil
}
