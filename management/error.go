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
	"fmt"
	"net/http"
	"strconv"
)

// Define common error ids
const (
	ERR_NOT_IMPLEMENTED         string = "not_implemented"
	ERR_BAD_REQUEST                    = "bad_request"
	ERR_SERVICE_ERROR                  = "service_error"
	ERR_BAD_GATEWAY                    = "external_error"
	ERR_NOT_FOUND                      = "not_found"
	ERR_FORBIDDEN                      = "forbidden_error"
	ERR_SERVICE_UNAVAILABLE            = "service_unavailable"
	ERR_RESOURCE_ALREADY_EXISTS        = "resource_already_exists"
)

// Defined common management Errors
var (
	ErrorNotImplemented = &Error{HttpStatus: http.StatusNotImplemented, Id: ERR_NOT_IMPLEMENTED, Description: "This version of the service does not support the functionality required to fulfill the request."}
	ErrorBadRequest     = &Error{HttpStatus: http.StatusBadRequest, Id: ERR_BAD_REQUEST, Description: "The request is missing a required parameter, includes an invalid parameter value, includes a parameter more than once, or is otherwise malformed."}
	ErrorInternal       = &Error{HttpStatus: http.StatusInternalServerError, Id: ERR_SERVICE_ERROR, Description: "The service encountered an unexpected condition that prevented it from fulfilling the request."}
	ErrorExternal       = &Error{HttpStatus: http.StatusBadGateway, Id: ERR_BAD_GATEWAY, Description: "The services, while acting as a gateway, received an invalid response from the upstream service."}
	ErrorNotFound       = &Error{HttpStatus: http.StatusNotFound, Id: ERR_NOT_FOUND, Description: "The service did not find the resource matching the Request-URI."}
	ErrorForbidden      = &Error{HttpStatus: http.StatusForbidden, Id: ERR_FORBIDDEN, Description: "Access to view or modify the resource identified by the Request-URI is forbidden."}
)

// Helper method used to create a custom bad-request error.
func GetBadRequestError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusBadRequest,
		Id:          ERR_BAD_REQUEST,
		Description: description,
	}
}

// Helper method used to create a custom internal error.
func GetInternalError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusInternalServerError,
		Id:          ERR_SERVICE_ERROR,
		Description: description,
	}
}

func GetResourceAlreadyExistsError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusConflict,
		Id:          ERR_RESOURCE_ALREADY_EXISTS,
		Description: description,
	}
}

// Helper method used to create a custom external error.
func GetExternalError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusBadGateway,
		Id:          ERR_BAD_GATEWAY,
		Description: description,
	}
}

// Helper method used to create a custom not found error.
func GetNotFoundError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusNotFound,
		Id:          ERR_NOT_FOUND,
		Description: description,
	}
}

// Helper method used to create custom forbidden errors.
func GetForbiddenError(description string) *Error {
	return &Error{
		HttpStatus:  http.StatusForbidden,
		Id:          ERR_FORBIDDEN,
		Description: description,
	}
}

// Helper method used to create custom 503 errors.
func GetServiceUnavailableError(description string, retryAfter int64) *Error {
	h := make(http.Header)
	h.Set("retry-after", strconv.FormatInt(retryAfter, 10))
	return &Error{
		HttpStatus:  http.StatusServiceUnavailable,
		Id:          ERR_SERVICE_UNAVAILABLE,
		Description: description,
		Header:      h,
	}
}

// Defines the type used to represent REST API error message body.
// Note that in order to help clients handle error conditions this
// structure follows the Oauth 2.0 definition.
type Error struct {
	// http status (e.g., http.StatusNotFound)
	HttpStatus int `json:"-"`

	// optional http headers, e.g. "retry_after"
	Header http.Header `json:"-"`

	// a freeform error identifier (e.g., 'BAD_PARAMETER')
	Id string `json:"error" binding:"required"`

	// the error identifier description
	Description string `json:"error_description" binding:"required"`

	// the error uri describing the error and recovery
	Uri string `json:"error_uri,omitempty"`
}

func NewError(status int, id, template string, args ...interface{}) *Error {
	return &Error{
		HttpStatus:  status,
		Id:          id,
		Description: fmt.Sprintf(template, args...),
	}
}

func (this *Error) String() string {
	return fmt.Sprintf("%v: %v", this.Id, this.Description)
}

func (this *Error) Error() string {
	return this.String()
}
