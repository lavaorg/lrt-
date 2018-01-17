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

package marathon

import (
	"github.com/gambol99/go-marathon"
	"github.com/verizonlabs/northstar/pkg/config"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

type MarathonClient struct {
	client marathon.Marathon
}

type NorthstarMarathonClient interface {
	CreateApplication(app *marathon.Application) error
	DeleteApplication(appName string) error
	DeleteGroup(groupName string) error
}

func NewMarathonClient() (*MarathonClient, error) {
	cfg := marathon.NewDefaultConfig()
	cfg.URL, _ = config.GetString("MARATHON_BASE_URL", "http://10.1.42.1:19090")
	cfg.HTTPBasicAuthUser, _ = config.GetString("MARATHON_USERNAME", "dcos")
	cfg.HTTPBasicPassword, _ = config.GetString("MARATHON_PASSWORD", "vzdcos")
	cfg.AuthType, _ = config.GetString("MARATHON_AUTH_TYPE", "BASIC_AUTH")
	cfg.AuthUrl, _ = config.GetString("MARATHON_AUTH_URL", "http://10.1.13.1:9090/acs/api/v1/auth/login")
	cfg.HTTPClient = management.NewHttpClient()

	client, err := marathon.NewClient(cfg)
	if err != nil {
		mlog.Error("Failed to create marathon client: %v", err)
		return nil, err
	}

	return &MarathonClient{client: client}, nil
}

func (m *MarathonClient) CreateApplication(app *marathon.Application) error {
	mlog.Debug("App: %v", app.String())
	_, err := m.client.UpdateApplication(app, true)
	return err
}

func (m *MarathonClient) DeleteApplication(appName string) error {
	mlog.Debug("App delete: %v", appName)
	_, err := m.client.DeleteApplication(appName, true)
	return err
}

func (m *MarathonClient) DeleteGroup(groupName string) error {
	mlog.Debug("Group delete: %v", groupName)
	_, err := m.client.DeleteGroup(groupName, true)
	return err
}
