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
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	engineLock sync.RWMutex
	engine     *gin.Engine
	client     = NewHttpClient()
)

// Engine
// Return the one and only gin engine
func Engine() *gin.Engine {
	if engine == nil {
		engineLock.Lock()
		defer engineLock.Unlock()
		if engine == nil {
			//gin API takes care of value checking
			// mgmt pkg wants default as release mode
			// so set it.
			value := os.Getenv(gin.GIN_MODE)
			if len(value) == 0 {
				gin.SetMode(gin.ReleaseMode)
			} else {
				gin.SetMode(value)
			}
			engine = gin.New()
		}
	}
	return engine
}

// Listen
// Listen to the given port (e.g., ":8888") and optional
// interface (designated by IP, e.g., lo0 using "127.0.0.1:8888")
func Listen(hostAndPort string) error {
	if err := Engine().Run(hostAndPort); err != nil {
		return err
	}
	return nil
}

// PostJSON
// Perform a HTTP POST using JSON content marshaled from the given content
// returning response content as a byte array
func PostJSON(hostAndPort, path string, content interface{}) ([]byte, *Error) {
	return PostJSONWithHeaders(hostAndPort, path, content, nil)
}

// PostJSONWithHeaders
// Perform a HTTP POST using JSON content marshaled from the given content
// returning response content as a byte array
func PostJSONWithHeaders(hostAndPort, path string, content interface{}, headers map[string]string) ([]byte, *Error) {

	// marshal content
	body, err := json.Marshal(content)
	if err != nil {
		return nil, NewError(http.StatusBadRequest, "bad_request", err.Error())
	}
	buffer := bytes.NewBuffer(body)

	// build request
	request, err := http.NewRequest("POST", hostAndPort+path, buffer)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}
	request.Header.Set("Content-Type", "application/json")
	if nil != headers {
		for key, value := range headers {
			request.Header.Set(key, value)
		}
	}

	// process request
	return process(request)
}

// Post
// Perform an empty HTTP POST
// returning response content as a byte array
func Post(hostAndPort, path string) ([]byte, *Error) {

	// build request
	request, err := http.NewRequest("POST", hostAndPort+path, nil)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	// process request
	return process(request)
}

// Post
// Perform HTTP POST request with data
// returning response content as a byte array
func PostData(hostAndPort, path string, content_type string, data []byte) ([]byte, *Error) {

	buffer := bytes.NewBuffer(data)

	// build request
	request, err := http.NewRequest("POST", hostAndPort+path, buffer)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	request.Header.Set("Content-Type", content_type)

	// process request
	return process(request)
}

// PostDataWithHeaders
// Perform a HTTP POST using the data
// returning response content as a byte array
func PostDataWithHeaders(hostAndPort, path string, data []byte, headers map[string]string) ([]byte, *Error) {
	buffer := bytes.NewBuffer(data)

	// build request
	request, err := http.NewRequest("POST", hostAndPort+path, buffer)

	if err != nil {
		return nil, NewError(requestCreationFailed, "Unable to create request", err.Error())
	}

	if nil != headers {
		for key, value := range headers {
			request.Header.Set(key, value)
		}
	}

	// process request
	return process(request)
}

// PutJSON
// Perform a HTTP PUT using the JSON content marsheled from the given content
// returning response content as a byte array
func PutJSON(hostAndPort, path string, content interface{}) ([]byte, *Error) {
	return PutJSONWithHeaders(hostAndPort, path, content, nil)
}

// PutJSONWithHeaders
// Perform a HTTP PUT using the JSON content marsheled from the given content
// returning response content as a byte array
func PutJSONWithHeaders(hostAndPort, path string, content interface{}, headers map[string]string) ([]byte, *Error) {

	// marshal content
	body, err := json.Marshal(content)
	if err != nil {
		return nil, NewError(http.StatusBadRequest, "bad_request", err.Error())
	}
	buffer := bytes.NewBuffer(body)

	// build request
	request, err := http.NewRequest("PUT", hostAndPort+path, buffer)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}
	request.Header.Set("Content-Type", "application/json")
	if nil != headers {
		for key, value := range headers {
			request.Header.Set(key, value)
		}
	}

	// process request
	return process(request)
}

// UploadFile: uploading a given file to server location
// Perform a HTTP POST using bytes buffer content
// returning response content as a byte array
func UploadFile(hostAndPort, path string, filename string) ([]byte, *Error) {

	bodyBuf := &bytes.Buffer{}
	bufWriter := bufio.NewWriter(bodyBuf)
	// open file handle
	fh, err := os.Open(filename)
	defer fh.Close()

	if err != nil {
		return nil, NewError(http.StatusInternalServerError, "Unable to open file", err.Error())
	}
	//iocopy
	_, err = io.Copy(bufWriter, fh)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, "Unable to copy file", err.Error())
	}

	// build request
	request, err := http.NewRequest("POST", hostAndPort+path, bodyBuf)

	if err != nil {
		return nil, NewError(requestCreationFailed, "Unable to create request", err.Error())
	}
	request.Header.Set("FileName", filepath.Base(filename))

	// process request
	return process(request)

}

// Put
// Perform an empty HTTP PUT
// returning response content as a byte array
func Put(hostAndPort, path string) ([]byte, *Error) {

	// build request
	request, err := http.NewRequest("PUT", hostAndPort+path, nil)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	// process request
	return process(request)
}

// Get
// Perform a HTTP GET
// returning response content as a byte array
func Get(hostAndPort, path string) ([]byte, *Error) {
	return GetWithHeaders(hostAndPort, path, nil)
}

// Get
// Perform a HTTP GET with headers
// returning response content as a byte array
func GetWithHeaders(hostAndPort, path string, headers map[string]string) ([]byte, *Error) {

	// build request
	request, err := http.NewRequest("GET", hostAndPort+path, nil)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	if nil != headers {
		for key, value := range headers {
			request.Header.Set(key, value)
		}
	}

	// process request
	return process(request)
}

// Delete
// Perform a HTTP DELETE
func Delete(hostAndPort, path string) *Error {
	return DeleteWithHeaders(hostAndPort, path, nil)
}

// Delete
// Perform a HTTP DELETE.  Adds headers to request if provided.
func DeleteWithHeaders(hostAndPort, path string, headers map[string]string) *Error {

	// build request
	request, err := http.NewRequest("DELETE", hostAndPort+path, nil)
	if err != nil {
		return NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	// Add headers to request
	if nil != headers {
		for key, value := range headers {
			request.Header.Set(key, value)
		}
	}

	// process request
	if _, err := process(request); err != nil {
		return err
	}
	return nil
}

// process
// Common HTTP client Do( request ) and parsing of any Error responses
func process(request *http.Request) (result []byte, merr *Error) {

	// perform request
	response, err := client.Do(request)
	if err != nil {
		return nil, NewError(http.StatusBadGateway, "bad_gateway", err.Error())
	}

	// pull any response content
	if response.Body != nil {
		defer response.Body.Close()
		result, err = ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, NewError(http.StatusPartialContent, "partial_content", err.Error())
		}
	}

	// process any errors, and if so, attempt to interpret the response
	// as a management.Error JSON and return.
	if response.StatusCode >= http.StatusBadRequest {
		defer func() {
			if r := recover(); r != nil {
				response = nil
			}
		}()
		merr = NewError(response.StatusCode, http.StatusText(response.StatusCode), string(result))
		if len(result) > 0 {
			// if this unmarshal panic's, then the defer function
			// will correctly return a default error created above
			json.Unmarshal(result, merr)
		}
		return nil, merr
	}

	// return ok
	return result, nil
}
