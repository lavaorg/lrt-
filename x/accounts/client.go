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

package accounts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lavaorg/lrt/x/management"
	"github.com/lavaorg/lrt/x/mlog"
	"io/ioutil"
	"net/http"
)

// Defines the base http client used by the library.
type BaseClient struct {
	httpClient *http.Client
	baseUrl    string
}

// Helper method used to execute a patch request.
func (client *BaseClient) executeHttpMethod(method string, url string, accessToken string, resourceObject interface{}) (body []byte, err *management.Error) {
	mlog.Debug("executeHttpMethod %s", url)

	// Parse the resource object (if any) as request body.
	var request *http.Request

	if resourceObject != nil {
		requestBody, goErr := json.Marshal(resourceObject)

		if goErr != nil {
			return nil, management.GetInternalError(goErr.Error())
		}

		buffer := bytes.NewBuffer(requestBody)
		request, _ = http.NewRequest(method, url, buffer)
	} else {
		request, _ = http.NewRequest(method, url, nil)
	}

	//NOTE: requires the charset=utf-8 to be set. Failure to do so results in obscure errors such as failure to unmarshal. See: NPDDCS-2983
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Close = true

	// If token was provided, add to the request header
	if accessToken != "" {
		mlog.Debug("Setting request authorization token.")
		request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	}

	response, goErr := client.httpClient.Do(request)

	// Check go errors executing the request.
	if goErr != nil {
		return nil, management.GetInternalError(goErr.Error())
	}

	// Check for HTTP client or service errors.
	if response.StatusCode >= 300 {
		err := management.GetExternalError(response.Status)

		if response.Body != nil {
			defer response.Body.Close()
			body, _ := ioutil.ReadAll(response.Body)
			json.Unmarshal(body, err)
		}

		return nil, err
	}

	// Parse the body if and only if one was provided.
	if response.Body != nil {
		defer response.Body.Close()
		body, _ = ioutil.ReadAll(response.Body)
	}

	return body, nil
}
