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

package main

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	vaultlib "github.com/verizonlabs/northstar/pkg/vaultlib"
)

func main() {
	mlog.EnableDebug(true)
	mlog.Debug("Starting the sampleapp")
	if secretInfo := vaultlib.FetchSecret(); secretInfo != nil {
		svc := "dummyCredsSvc"
		if creds := secretInfo.Credentials(svc); creds == nil {
			mlog.Error("No Credentials found for svc[%s] @ secret store", svc)
		} else {
			//NOTE::::Please not printing credential value here is to showcase which variables need to be accessed for credentials. In service code DO NOT print them
			mlog.Info("For svc %s Username [%s] Password [%s]", svc, creds.User, creds.Pwd)
		}

		svc = "dummyKeySvc"
		if key := secretInfo.Keys(svc); key == nil {
			mlog.Error("No Keys found for svc[%s] @ secret store", svc)
		} else {
			//NOTE::::Please not printing keys value here is to showcase which variable need to be accessed for keys. In service code DO NOT print them
			mlog.Info("For svc %s, keys [%s]", svc, key.Key)
		}

		svc = "dummyCertsSvc"
		if certs := secretInfo.Certificates(svc); certs == nil {
			mlog.Error("No Certificates found for svc[%s] @ secret store", svc)
		} else {
			//NOTE::::Please not printing certificates value here is to showcase which variable need to be accessed for certificates. In service code DO NOT print them
			mlog.Info("For svc %s, certs [%s]", svc, certs.Cert)
		}

		svc = "dummyOtherSvc"
		param := "ip"
		if creds := secretInfo.GenericSecret(svc, param); creds == nil {
			mlog.Error("No Secret named [%s] found for svc[%s] @ secret store", param, svc)
		} else {
			//NOTE::::Please not printing generic secret value here is to showcase which variable need to be accessed for generic secret. In service code DO NOT print them
			mlog.Info("For svc %s [%s] : [%s] ", svc, param, creds.Generic)
		}
	} else {
		mlog.Info("No secrets found")
	}

	for {
	}
}
