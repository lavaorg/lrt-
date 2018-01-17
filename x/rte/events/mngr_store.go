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

package events

import (
	"github.com/orcaman/concurrent-map"
	"github.com/verizonlabs/northstar/pkg/rte/topics"
)

type ManagerStore interface {
	GetManager(runtime string) (SnippetManager, error)
}

type SnippetManagerStore struct {
	serviceName string
	managers    cmap.ConcurrentMap
}

func NewSnippetMngrStore(serviceName string) *SnippetManagerStore {
	return &SnippetManagerStore{serviceName: serviceName, managers: cmap.New()}
}

func (p SnippetManagerStore) GetManager(runtime string) (SnippetManager, error) {
	topic, err := topics.GetCtrlTopicByType(runtime)
	if err != nil {
		return nil, err
	}

	manager, ok := p.managers.Get(topic)
	if !ok {
		manager, err = NewSnippetManagerService(p.serviceName, topic)
		if err != nil {
			return nil, err
		}
		p.managers.Set(topic, manager)
	}

	return manager.(SnippetManager), nil
}
