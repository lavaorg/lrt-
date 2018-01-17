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

package lualib

import (
	"encoding/json"
	"fmt"

	marathon "github.com/gambol99/go-marathon"
	"github.com/yuin/gopher-lua"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

var errorMarathonNotConfigured = "MARATHON Client not configured"

type ScaleAppCtx struct {
	DeploymentID string `json:"deployment_id"`
	NumInstances int    `json:"num_instances"`
}

type UpdateAppEnvCtx struct {
	DeploymentID string `json:"deployment_id"`
	EnvVariables []struct {
		EnvKey   string `json:"env_key"`
		EnvValue string `json:"env_value"`
	} `json:"env_variables"`
}

type marathonModule struct {
	client marathon.Marathon
}

func NewMarathonModule(marathonClient marathon.Marathon) *marathonModule {
	return &marathonModule{
		client: marathonClient,
	}
}

func (m *marathonModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"create_app":        m.CreateApp,
		"scale_app":         m.ScaleApp,
		"scale_custom_app":  m.ScaleCustomApp,
		"delete_app":        m.DeleteApp,
		"update_app_env":    m.UpdateAppEnv,
		"last_task_failure": m.GetLastTaskFailure,
		"get_app_health":    m.GetAppHealth,
		"get_app":           m.GetAppDetails,
	})
	L.Push(mod)
	return 1
}

func (m *marathonModule) CreateApp(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	jsoninfo := L.CheckString(1)

	var application marathon.Application
	err := json.Unmarshal([]byte(jsoninfo), &application)

	_, err = m.client.CreateApplication(&application)
	if err != nil {
		mlog.Alarm("Failed to launch marathon job %v", err)
	} else {
		m.client.WaitOnApplication(application.ID, 0)
		mlog.Debug("Successfully launched marathon application: %s", application.ID)
	}
	L.Push(lua.LNil)
	return 1
}

func (m *marathonModule) DeleteApp(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	appId := L.CheckString(1)
	_, err1 := m.client.DeleteApplication(appId, true)
	if err1 != nil {
		mlog.Error("Failed to kill marathon app: %s, error: %s", appId, err1)
	}
	mlog.Debug("Killed marathon app called with app id=%s", appId)
	L.Push(lua.LNil)
	return 1
}

func (m *marathonModule) ScaleApp(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	appId := L.CheckString(1)
	instances := L.CheckInt(2)
	force := false
	if L.GetTop() == 3 {
		force = L.CheckBool(3)
	}

	_, err := m.client.ScaleApplicationInstances(appId, instances, force)

	if err != nil {
		mlog.Error("Failed to scale up the application: %s, error: %s", appId, err)
	} else {
		mlog.Debug("Waiting on application to launch %s", appId)
		m.client.WaitOnApplication(appId, 0)
		mlog.Debug("Successfully scaled up the application: %s", appId)
	}
	L.Push(lua.LNil)
	return 1
}

func (m *marathonModule) ScaleCustomApp(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	param := L.CheckString(1)
	paramByte := []byte(param)
	var scaleApp ScaleAppCtx
	err := json.Unmarshal(paramByte, &scaleApp)
	if err != nil {
		mlog.Error("Error to unmarshal scale app", err)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 1
	}
	appId := scaleApp.DeploymentID
	numInstances := scaleApp.NumInstances
	_, err = m.client.ScaleUpApplicationInstances(appId, numInstances, true)

	if err != nil {
		mlog.Error("Failed to scale up the application: %s, error: %s", appId, err)
	} else {
		mlog.Debug("Waiting on application to launch %s", appId)
		m.client.WaitOnApplication(appId, 0)
		mlog.Debug("Successfully scaled up the application: %s", appId)
	}
	L.Push(lua.LNil)
	return 1
}

func (m *marathonModule) UpdateAppEnv(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	param := L.CheckString(1)
	paramByte := []byte(param)
	var updateAppEnv UpdateAppEnvCtx
	err := json.Unmarshal(paramByte, &updateAppEnv)
	if err != nil {
		mlog.Error("Error to unmarshal scale app", err)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 1
	}
	appId := updateAppEnv.DeploymentID
	envVariables := updateAppEnv.EnvVariables

	application, err := m.client.Application(appId)
	application.Fetch = nil
	application.Tasks = nil
	application.Version = ""
	application.LastTaskFailure = nil

	if err != nil {
		mlog.Error("Failed to fetch application: %s, error: %s", appId, err)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 1
	}

	for _, v := range envVariables {
		application.AddEnv(v.EnvKey, v.EnvValue)
	}
	_, err = m.client.UpdateApplication(application, true)

	if err != nil {
		mlog.Error("Failed to update environment for the application: %s, error: %s", appId, err)
		L.Push(lua.LString(fmt.Sprintf("%s", err)))
		return 1
	} else {
		mlog.Debug("Waiting on application to launch %s", appId)
		m.client.WaitOnApplication(appId, 0)
		mlog.Debug("Successfully updated the application environment: %s", appId)
	}
	L.Push(lua.LNil)
	return 1

}

func (m *marathonModule) GetLastTaskFailure(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	appId := L.CheckString(1)
	application, err1 := m.client.Application(appId)
	if err1 != nil {
		mlog.Error("Failed to fetch application: %s, error: %s", appId, err1)
		L.Push(lua.LString(fmt.Sprintf("%s", err1)))
		return 1
	}
	lastTaskFailure, _ := json.Marshal(application.LastTaskFailure)
	L.Push(lua.LString(string(lastTaskFailure)))
	return 1
}

type AppHealth struct {
	Healthy_Tasks   int
	Unhealthy_Tasks int
}

func (m *marathonModule) GetAppHealth(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	appId := L.CheckString(1)
	application, err1 := m.client.Application(appId)
	if err1 != nil {
		mlog.Error("Failed to fetch application: %s, error: %s", appId, err1)
		L.Push(lua.LString(fmt.Sprintf("%s", err1)))
		return 1
	}
	appHealth, _ := json.Marshal(AppHealth{
		Healthy_Tasks:   application.TasksHealthy,
		Unhealthy_Tasks: application.TasksUnhealthy,
	})
	L.Push(lua.LString(string(appHealth)))
	return 1
}

func (m *marathonModule) GetAppDetails(L *lua.LState) int {
	if m.client == nil {
		L.Push(lua.LString(errorMarathonNotConfigured))
		return 1
	}

	appId := L.CheckString(1)
	application, err1 := m.client.Application(appId)
	if err1 != nil {
		mlog.Error("Failed to fetch application: %s, error: %s", appId, err1)
		L.Push(lua.LNil)
		L.Push(lua.LString(fmt.Sprintf("%s", err1)))
		return 2
	}
	app, _ := json.Marshal(application)
	L.Push(lua.LString(string(app)))
	return 1
}
