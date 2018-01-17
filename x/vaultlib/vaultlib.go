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

package vaultlib

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/channelmeter/vault-gatekeeper-mesos/gatekeeper"
	"github.com/franela/goreq"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/vaultlib/config"
)

const (
	vaultReconnectDelayTimeSec = time.Duration(5)
)

const (
	SECRET_TYPE_CREDENTIALS  = 1
	SECRET_TYPE_KEYS         = 2
	SECRET_TYPE_CERTIFICATES = 3
	SECRET_TYPE_OTHERS       = 4
)

var SecretTypeString []string = []string{"", "creds", "keys", "certs", "others"}

type SecretData map[string]interface{}
type AppSecretData map[string]SecretData // Info kept @ ServicesSecretPath

// Wrappers for differet Secret Types

// This structure is used if the secret stored by service is custom one
// Secret for the vaultlib is always a string. Application is expected to do
// encoding if required
type SecretGeneric struct {
	Generic string "json:generic_secret"
}

// This structure is used to store secret of Key type.
type SecretKey struct {
	Key string `json:"key"`
}

// This structure is used to store secret of Certificate type.
type SecretCertificate struct {
	Cert string `json:"cert"`
}

// This structure is used to store secret of Credential type.
type SecretCredential struct {
	User string `json:"user"`
	Pwd  string `json:"pwd"`
}

// For Decoded Secrets received by services. Wrapper for info kept at ServicesSecretPath
type DecodedServiceSecretInfo struct {
	AppId   string     `json:"app_id"`
	Secrets SecretData `json:"secrets"` // This is actually of type AppSecretData
}

// Secret info for individual service. Info kept @ SecretPath
type ServiceSecretInfo struct {
	Svc    string     `json:"svc"`
	Secret SecretData `json:"secret"`
}

// For Request of Service Publish Info
type ServiceSecretReqInfo struct {
	Svc        string   `json:"svc"`
	SecretType []string `json:"secret_type"`
}

type ServicePublishReqInfo struct {
	DC            string                 `json:"dc"`
	DevDeployment bool                   `json:"dev"`
	AppId         string                 `json:"app_id"`
	ReqSecretApps []ServiceSecretReqInfo `json:"req_secrets"`
}

type ServicePublishReq struct {
	Info []ServicePublishReqInfo `json:"info"`
}

type VaultRequest struct {
	goreq.Request
}

func (r VaultRequest) Do() (*goreq.Response, error) {
	resp, err := r.Request.Do()
	for err == nil && resp.StatusCode == 307 {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		r.Request.Uri = resp.Header.Get("Location")
		resp, err = r.Request.Do()
	}
	return resp, err
}

type VaultStatus struct {
	Initialized    bool `json:"initialized"`
	Sealed         bool `json:"sealed"`
	NoOfSealKeys   int  `json:"n"`
	Threshold      int  `json:"t"`
	UnsealProgress int  `json:"progress"`
}

type GatekeeperStatus struct {
	Sealed string `json:"status"`
	Ok     bool   `json:"ok"`
}

type vaultError struct {
	Code   int      `json:"-"`
	Errors []string `json:"errors"`
}

func checkVaultStatus(vaultHostPortArr string) (string, error) {
	for _, host := range strings.Split(vaultHostPortArr, ",") {
		var uri string
		if config.VaultTlsEnabled == true {
			uri = "https://" + host + "/v1/sys/seal-status"
		} else {
			uri = "http://" + host + "/v1/sys/seal-status"
		}
		r, err := VaultRequest{goreq.Request{
			Uri:             uri,
			Method:          "GET",
			MaxRedirects:    10,
			Insecure:        true,
			RedirectHeaders: true,
		}}.Do()
		if err != nil {
			mlog.Error("Error getting vault status from %s: %v", host, err)
			continue
		}
		defer r.Body.Close()
		if r.StatusCode == 200 {
			var status VaultStatus
			if err := r.Body.FromJsonTo(&status); err == nil {
				if status.Sealed == false {
					return host, nil
				} else {
					mlog.Error("Vault instance on %s is sealed", host)
				}
			} else {
				mlog.Error("Error getting vault status. Error parsing body: %v", err)
			}
		} else {
			var e vaultError
			e.Code = r.StatusCode
			if err := r.Body.FromJsonTo(&e); err == nil {
				mlog.Error(strings.Join(e.Errors, ","))
			} else {
				mlog.Error("Error getting vault status. Communication Error")
			}
		}
	}
	return "", errors.New("Vault communication error.. check if vault is initialized and unsealed")

}

func checkGatekeeperStatus(gatekeeperHostPortArr string) (string, error) {
	for _, host := range strings.Split(gatekeeperHostPortArr, ",") {
		var uri string
		if config.GatekeeperTlsEnabled == true {
			uri = "https://" + host + "/status.json"
		} else {
			uri = "http://" + host + "/status.json"
		}
		r, err := VaultRequest{goreq.Request{
			Uri:             uri,
			Method:          "GET",
			MaxRedirects:    10,
			Insecure:        true,
			RedirectHeaders: true,
		}}.Do()
		if err != nil {
			mlog.Error("Error getting gatekeeper status from %s: %v", host, err)
			continue
		}
		defer r.Body.Close()
		if r.StatusCode == 200 {
			var status GatekeeperStatus
			if err := r.Body.FromJsonTo(&status); err == nil {
				if status.Sealed == "Unsealed" {
					return host, nil
				} else {
					mlog.Error("Gatekeeper instance on %s is sealed", host)
				}
			} else {
				mlog.Error("Error getting gatekeeper status. Error parsing body: %v", err)
			}
		} else {
			mlog.Error("Error getting gatekeeper status. Communication Error")
		}
	}
	return "", errors.New("Gatekeeper communication error.. check if gatekeeper is unsealed")

}

func getGatekeeperClient(gatekeeperHostPortArr, vaultHostPortArr string) (*gatekeeper.Client, error) {
	var vaultAddress, gatekeeperAddress string
	if vaultHostPort, err := checkVaultStatus(vaultHostPortArr); err == nil {
		if config.VaultTlsEnabled == true {
			vaultAddress = "https://" + vaultHostPort
		} else {
			vaultAddress = "http://" + vaultHostPort
		}
	} else {
		return nil, err
	}
	gatekeeperHostPort, err := checkGatekeeperStatus(gatekeeperHostPortArr)
	if err != nil {
		return nil, err
	}
	if config.GatekeeperTlsEnabled == true {
		gatekeeperAddress = "https://" + gatekeeperHostPort
		if config.VaultCAPath == "" && config.VaultCACert == "" {
			gkClient, err := gatekeeper.NewClient(vaultAddress, gatekeeperAddress, nil)
			if err == nil {
				gkClient.InsecureSkipVerify(true)
			}
			return gkClient, err
		} else {
			LoadGkCA := func() (*x509.CertPool, error) {
				if config.VaultCAPath != "" {
					return gatekeeper.LoadCAPath(config.VaultCAPath)
				} else if config.VaultCACert != "" {
					return gatekeeper.LoadCACert(config.VaultCACert)
				}
				return nil, errors.New("invariant violation")
			}
			if certs, err := LoadGkCA(); err == nil {
				return gatekeeper.NewClient(vaultAddress, gatekeeperAddress, certs)
			} else {
				mlog.Error("Failed to read gatekeeper CA certs. Error: %v", err)
				return nil, err
			}
		}
	} else {
		gatekeeperAddress = "http://" + gatekeeperHostPort
		return gatekeeper.NewClient(vaultAddress, gatekeeperAddress, nil)
	}
}

func getVaultClient(vaultHostPortArr, vaultToken string) (*vaultapi.Client, error) {
	var vaultAddress string

	if config.VaultCAPath == "" && config.VaultCACert == "" {
		os.Setenv("VAULT_SKIP_VERIFY", "true")
	}

	if vaultHostPort, err := checkVaultStatus(vaultHostPortArr); err == nil {
		if config.VaultTlsEnabled == true {
			vaultAddress = "https://" + vaultHostPort
		} else {
			vaultAddress = "http://" + vaultHostPort
		}
	} else {
		return nil, err
	}

	client, err := vaultapi.NewClient(nil)
	if err != nil {
		return nil, err
	}
	client.SetAddress(vaultAddress)
	client.SetToken(vaultToken)
	client.SetMaxRetries(10)
	return client, nil
}

// Get keys for a particular service from information fetched from Vault for a service.
// Eg. This function gets nginx keys for nginx service, from the information fetched for nginx from Vault
func (ds *DecodedServiceSecretInfo) Keys(svc string) *SecretKey {
	var retKey SecretKey
	for key, val := range ds.Secrets {
		if svc == key {
			for k, v := range val.(map[string]interface{}) {
				if k == SecretTypeString[SECRET_TYPE_KEYS] {
					bb, _ := base64.StdEncoding.DecodeString(v.(string))
					if err := json.Unmarshal(bb, &retKey); err != nil {
						mlog.Error("Unmarshal error %+v", err)
					} else {
						return &retKey
					}
				}
			}
		}
	}
	return nil
}

// Get credentials for a particular service from information fetched from Vault for a service.
// Eg. This function gets Cassandra Credentials for Data service, from the information fetched for Data from Vault
func (ds *DecodedServiceSecretInfo) Credentials(svc string) *SecretCredential {
	var creds SecretCredential
	for key, val := range ds.Secrets {
		if svc == key {
			for k, v := range val.(map[string]interface{}) {
				if k == SecretTypeString[SECRET_TYPE_CREDENTIALS] {
					var vv map[string]interface{}
					bb, _ := base64.StdEncoding.DecodeString(v.(string))
					if err := json.Unmarshal(bb, &vv); err != nil {
						mlog.Error("Unmarshal error %+v", err)
						return nil
					} else {
						if err := json.Unmarshal(bb, &creds); err != nil {
							mlog.Error("Unmarshal error %s", err)
						}
						return &creds
					}
				}
			}
		}
	}
	return nil
}

// Get certificates for a particular service from information fetched from Vault for a service.
// Eg. This function gets nginx certificates for nginx service, from the information fetched for nginx from Vault
func (ds *DecodedServiceSecretInfo) Certificates(svc string) *SecretCertificate {
	var retCert SecretCertificate
	for key, val := range ds.Secrets {
		if svc == key {
			for k, v := range val.(map[string]interface{}) {
				if k == SecretTypeString[SECRET_TYPE_CERTIFICATES] {
					bb, _ := base64.StdEncoding.DecodeString(v.(string))
					if err := json.Unmarshal(bb, &retCert); err != nil {
						mlog.Error("Unmarshal error %+v", err)
					} else {
						return &retCert
					}
				}
			}
		}
	}
	return nil
}

// Get custome info for a particular service from information fetched from Vault for a service.
// Eg. This function gets ip address for X service, from the information fetched for X from Vault
func (ds *DecodedServiceSecretInfo) GenericSecret(svc, secretType string) *SecretGeneric {
	var retGeneric SecretGeneric
	for key, val := range ds.Secrets {
		if svc == key {
			for k, v := range val.(map[string]interface{}) {
				if k == secretType {
					bb, _ := base64.StdEncoding.DecodeString(v.(string))
					if err := json.Unmarshal(bb, &retGeneric); err != nil {
						mlog.Error("Unmarshal error %+v", err)
					} else {
						return &retGeneric
					}
				}
			}
		}
	}
	return nil
}

// Function to fetch the secret from a given path in vault.
//
// Services shall call this function to fetch secret using token received from Gatekeeper
func FetchSecret() (secretInfo *DecodedServiceSecretInfo) {

	vaultToken := GetVaultToken(config.GatekeeperHostPort, config.VaultHostPort)

	return FetchSecretWithParam(config.VaultHostPort, vaultToken, config.AppDC, config.AppId, config.DevDeployment)
}

// Function to fetch the secret from a given path in vault.
//
// VaultAgent shall call this function directly
//
// vaultHostPort 	: IP:Port to Vault
// vaultToken		: Token used for reading the secret
// appDc			: DC where service is running
// aapId			: appId of the service requesting secret
// devDeployment	: whether to pick secrets from default path used for developement testing
func FetchSecretWithParam(vaultHostPort, vaultToken, appDc, appId string, devDeployment bool) (secretInfo *DecodedServiceSecretInfo) {
	svcPath := config.GetServiceSecretPath(appId, appDc, devDeployment)
	if vaultToken == "" || vaultHostPort == "" || svcPath == "" {
		return nil
	}

	mlog.Debug("Trying to fetch secret for app[%s] from path [%s]", appId, svcPath)
	if k := FetchSecretFromPath(vaultToken, vaultHostPort, svcPath); k != nil {
		secretInfo = new(DecodedServiceSecretInfo)
		secretInfo.AppId = appId
		secretInfo.Secrets = k
		return secretInfo
	} else {
		mlog.Error("No Info for %s", appId)
		return nil
	}
}

// Function to push the secret at a given path in vault.
// This function would be used by vault-agent
func PushSecretToVault(vaultToken, vaultHostPort, path, data string) error {
	for {
		client, err := getVaultClient(vaultHostPort, vaultToken)
		if err != nil {
			mlog.Alarm("Error initializing vault client: %s", err.Error())
			time.Sleep(vaultReconnectDelayTimeSec * time.Second)
			continue
		}
		c := client.Logical()
		if ret, err := c.Write(path, map[string]interface{}{"value": data}); err != nil {
			return errors.New(fmt.Sprintf("Error in putting secret at path[%s] error [%s]", path, err))
		} else {
			mlog.Debug("Successful updation of secret @ %s. Result is [%+v]", path, ret)
			return nil
		}
	}
}

// Function to fetch a secret from a given path in vault
// This function is called directly by vault-agent or by vault-lib internally
func FetchSecretFromPath(vaultToken, vaultHostPort, path string) map[string]interface{} {

	for {
		client, err := getVaultClient(vaultHostPort, vaultToken)
		if err != nil {
			mlog.Alarm("Error initializing vault client: %s", err.Error())
			time.Sleep(vaultReconnectDelayTimeSec * time.Second)
			continue
		}
		c := client.Logical()
		if ret, err := c.Read(path); err != nil {
			mlog.Error("Error in reading secret at path[%s] error [%s]", path, err)
		} else {
			if ret != nil {
				if ret.Data != nil {
					var vv SecretData
					bb, _ := base64.StdEncoding.DecodeString(ret.Data["value"].(string))
					if err := json.Unmarshal(bb, &vv); err != nil {
						mlog.Error("Unmarshal error %+v", err)
					} else {
						return vv
					}
				}
			}
		}
		break
	}
	return nil
}

// Function to del a secret from a given path in vault
// This function is called directly by vault-agent
func DelSecretFromPath(vaultHostPort, vaultToken, path string) error {
	var err error
	for {
		client, err := getVaultClient(vaultHostPort, vaultToken)
		if err != nil {
			mlog.Alarm("Error initializing vault client: %s", err.Error())
			time.Sleep(vaultReconnectDelayTimeSec * time.Second)
			continue
		}
		c := client.Logical()
		if _, err = c.Delete(path); err != nil {
			mlog.Error("Error in deleting secret at path[%s] error [%s]", path, err)
		}
		break
	}
	return err
}

// Function to fetch READ token for the service via Gatekeeper
func GetVaultToken(gatekeeperHostPort, vaultHostPort string) string {
	mlog.Debug("Authenticating with Vault")
	for {
		mlog.Info("Getting token from gatekeeper for mesosTaskId %s", config.AppMesosTaskId)
		client, err := getGatekeeperClient(gatekeeperHostPort, vaultHostPort)
		if err != nil {
			mlog.Alarm("Error initializing gatekeeper client: %s", err.Error())
			time.Sleep(vaultReconnectDelayTimeSec * time.Second)
			continue
		}
		token, err := client.RequestVaultToken(config.AppMesosTaskId)
		if err == nil {
			return token
		} else {
			mlog.Alarm("Gatekeeper couldn't fetch vault token: %s", err.Error())
			time.Sleep(vaultReconnectDelayTimeSec * time.Second)
		}
	}
}

func fetchAndCheckEnv(env string, mandatory bool) string {
	val := os.Getenv(env)
	if mandatory && len(val) <= 0 {
		mlog.Error("%s env variable is not set", env)
	}
	return val
}

func GetAes256Key() []byte {
	return []byte("AES256Key-32Characters1234567890")
}

func GetAes128Key() []byte {
	return []byte("AES128Key-16Char")
}
