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

package management

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/verizonlabs/northstar/pkg/mlog"
)

const (
	requestCreationFailed = 550
	maxIdleConections     = 50000

	//timeout is the maximum amount of time a dial will wait for a connect to complet
	dialTimeoutSec = 5
	reqTimeoutSec  = 60
)

var (
	use_http_proxy = false
)

// Helper method used to create new Http Clients.
func NewHttpClient() *http.Client {
	var err error
	var timeout int
	if timeout, err = strconv.Atoi(os.Getenv("TCP_CONN_DIAL_TIMEOUT")); err == nil {
		timeout = dialTimeoutSec
	}
	if useproxy := os.Getenv("USE_HTTP_PROXY"); useproxy != "" {
		use_http_proxy = true
	}
	mlog.Info("use_http_proxy value is %t", use_http_proxy)
	if use_http_proxy == true {
		return &http.Client{}
	}
	insecureClient := false
	if tlsSkipVerification := os.Getenv("TLS_SKIP_VERIFICATION"); tlsSkipVerification != "" {
		insecureClient, _ = strconv.ParseBool(tlsSkipVerification)
	}

	rspHeaderTimeout := reqTimeoutSec
	if v := os.Getenv("MANAGEMENT_RSP_HEADER_TIMEOUT_SECONDS"); v != "" {
		if t, err := strconv.Atoi(v); err == nil {
			rspHeaderTimeout = t
		}
	}
	mlog.Debug("Using ResponseHeaderTimeout:%d", rspHeaderTimeout)

	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConections,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureClient,
			},
			ResponseHeaderTimeout: time.Second * time.Duration(rspHeaderTimeout),
			Dial: func(netw, addr string) (net.Conn, error) {
				mlog.Event("Connecting to %s", addr)
				c, err := net.DialTimeout(netw, addr, time.Second*time.Duration(timeout))
				if err != nil {
					if strings.Compare(reflect.TypeOf(err).String(), "*net.OpError") == 0 {
						mlog.Event("Unable to connect to %s", addr)
					}
					mlog.Event("Disconnected from %s", addr)
					return nil, err
				}
				mlog.Event("Connected to %s", addr)
				return c, nil
			},
		},
	}
}
