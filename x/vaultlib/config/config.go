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

package config

import (
	"fmt"
	"github.com/verizonlabs/northstar/pkg/config"
	"strings"
    "net"
    "github.com/verizonlabs/northstar/pkg/mlog"
    "github.com/verizonlabs/northstar/pkg/utils"
)

var (
    VaultHostPort           = resolveAddressString("VAULT_HOST_PORT")
    GatekeeperHostPort      = resolveAddressString("GATEKEEPER_HOST_PORT")
	AppDC, _                = config.GetString("APP_DC", "")
	AppMesosTaskId, _       = config.GetString("MESOS_TASK_ID", "")
	VaultTlsEnabled, _      = config.GetBool("VAULT_TLS_ENABLED", false)
	GatekeeperTlsEnabled, _ = config.GetBool("GATEKEEPER_TLS_ENABLED", false)
	VaultCAPath, _          = config.GetString("VAULT_CAPATH", "")
	VaultCACert, _          = config.GetString("VAULT_CACERT", "")
	AppId, _                = config.GetString("APP_ID", "")
	DevDeployment, _        = config.GetBool("DEV_DEPLOYMENT", false)
	AppVaultBasePath        = GetAppVaultBasePath("", DevDeployment)
	SecretPath              = "%s/secrets/%s/%s/%s"  // <basePath>/secrets/<dc>/<env>/svcName
	ServicesSecretPath      = "%s/services/%s/%s/%s" // <basePath>/services/<dc>/<env>/svcName
	DefaultEnv              = "default"
)

func GetAppEnv(appId string, devDeployment bool) (string, string) {
	appEnv := strings.Split(strings.TrimPrefix(appId, "/"), "/")
	if len(appEnv) < 3 {
		return "", ""
	} else {
		if devDeployment == true {
			return DefaultEnv, strings.Join(appEnv[2:], "/")
		} else {
			return appEnv[1], strings.Join(appEnv[2:], "/")
		}
	}
}

func GetAppVertical(appId string) string {
	var appEnv []string
	if appId == "" {
		appEnv = strings.Split(strings.TrimPrefix(AppId, "/"), "/")
	} else {
		appEnv = strings.Split(strings.TrimPrefix(appId, "/"), "/")
	}
	if len(appEnv) < 3 {
		return ""
	} else {
		return appEnv[0]
	}
}

func GetAppVaultBasePath(app string, devDeployment bool) string {
	if devDeployment == true {
		return fmt.Sprintf("dev/%s", app)
	} else {
		return fmt.Sprintf("ts/%s", app)
	}
}

func GetServiceSecretPath(appId, appDc string, devDeployment bool) string {
	env, svc := GetAppEnv(appId, devDeployment)

	appVaultBasePath := GetAppVaultBasePath(GetAppVertical(appId), devDeployment)

	if env == "" || svc == "" || appVaultBasePath == "" || appDc == "" {
		return ""
	} else {
		return fmt.Sprintf(ServicesSecretPath, appVaultBasePath, appDc, env, svc)
	}
}

func GetSecretPath(app, appDc, env, svc string, devDeployment bool) string {
	if devDeployment == true {
		env = DefaultEnv
	}

	appVaultBasePath := GetAppVaultBasePath(app, devDeployment)

	if env == "" || svc == "" || appVaultBasePath == "" || appDc == "" {
		return ""
	} else {
		return fmt.Sprintf(SecretPath, appVaultBasePath, appDc, env, svc)
	}
}
// Api to resolve string of hostnames - to ip addresses
// hostnames are expected to have a ":port" part at the end
func resolveAddressString(param string) (string) {
        addressStr, _ := config.GetString(param, "")
        var newAddressStr string
        if (len(addressStr)<= 0) {
                return ""
        }
        for _, host := range strings.Split(addressStr, ",") {
                s := strings.Split(host, ":")
                if (len(s)<=1){
                        mlog.Error("Expected ip/Host:Port %s\n",s[0])
                       return ""
               }
                ipOrHost, port := s[0], s[1]
                addr := net.ParseIP(ipOrHost)
                if addr == nil {
                        mlog.Info("Resolving hostname: %s to IP address\n", host)
                        ip_part := utils.HostsToIps(ipOrHost)
                        host = ip_part+":"+port
                        mlog.Info("New hostname and port are : %s\n", host)
                }
                if (len(newAddressStr)<= 0) {
                        newAddressStr=host
                } else {
                        newAddressStr+=","+host
                }
        }
        return newAddressStr
}
