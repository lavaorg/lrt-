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

package middleware

import (
	"errors"
	"github.com/gin-gonic/gin"
	"net/http"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/thingspace"
	. "github.com/verizonlabs/northstar/pkg/thingspace/api"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"strings"
)

const (
	// Defines the key of the context loginname.
	LOGINNAME_KEY string = "loginname"

	// Defines the key of the context clientid.
	CLIENTID_KEY string = "clientid"
)

var (
	// Defines the authorization errors.
	ErrorUnauthorizedClient = &management.Error{HttpStatus: http.StatusUnauthorized, Id: "unauthorized_client", Description: "The client has not authorized the requested resource path using this token."}
	ErrorInvalidClient      = &management.Error{HttpStatus: http.StatusUnauthorized, Id: "invalid_client", Description: "The request is missing a required parameter, includes an invalid parameter value, includes a parameter more than once, or is otherwise malformed."}
	ErrorExpiredToken       = &management.Error{HttpStatus: http.StatusBadRequest, Id: "expired_token", Description: "The request token has expired."}
	ErrorUnavailableToken   = &management.Error{HttpStatus: http.StatusUnauthorized, Id: "unavailable_token", Description: "The request is missing the required Bearer Authorization token."}
)

// Defines the type used to represent collection of allowed services.
type AllowedServices map[string][]Resource

// Defines the type used to represent a request resource.
type Resource struct {
	Methods []string
	Paths   []string
}

// Middleware for intersecting the request
func Authorization(accessData *AllowedServices, authClient thingspace.AuthClient) gin.HandlerFunc {

	return func(context *gin.Context) {
		mlog.Debug("Authorization")

		request := context.Request
		writer := context.Writer

		// Get the token from the header of the request
		bearerToken, err := getBearerToken(request)

		if err != nil {
			mlog.Error("Failed to get Authorization Bearer token with error: %s.", err.Error())
			writer.Header().Set("WWW-Authenticate", "Bearer realm=Restricted")
			context.JSON(http.StatusUnauthorized, ErrorUnavailableToken)
			context.Abort()
		} else {
			mlog.Debug("Validating Authorization Bearer token.")

			// Get the expected Authorization Bearer Token.
			tokenInfo, mgtError := authClient.GetTokenInfo(bearerToken)

			if mgtError != nil {
				mlog.Error("Failed to get token information with error: %s", mgtError.Description)
				context.JSON(mgtError.HttpStatus, mgtError)
				context.Abort()
				return
			}

			// Validate the token
			if mgtError = validateToken(writer, request, tokenInfo, accessData); mgtError == nil {

				context.Set(CLIENTID_KEY, tokenInfo.ClientId)
				context.Set(LOGINNAME_KEY, tokenInfo.UserName)

				// Call next
				context.Next()
			} else {
				mlog.Error("Failed to validate Authorization Bearer token with error: %s", mgtError.Description)
				context.JSON(mgtError.HttpStatus, mgtError)
				context.Abort()
			}
		}
	}
}

// Helper method used to extract the Authorization Bearer token from the request.
func getBearerToken(request *http.Request) (string, error) {
	authHeader := request.Header.Get("Authorization")

	if authHeader == "" {
		return "", errors.New("Error, missing authorization header.")
	}

	values := strings.SplitN(authHeader, " ", 2)
	if len(values) != 2 || values[0] != "Bearer" {
		return "", errors.New("Error, invalid authorization token type.")
	}

	return values[1], nil
}

// Helper method used to validate the token info.
func validateToken(writer http.ResponseWriter, request *http.Request, tokenInfo *TokenInfo, accessData *AllowedServices) *management.Error {

	// Validate token is not expired.
	if tokenInfo.IsExpired() {
		mlog.Debug("The Authorization Bearer token is expired.")
		return ErrorExpiredToken
	}

	var scopes []string

	// Search for the required scope in the list of allowed services.
	for scope, resources := range *accessData {
		mlog.Debug("Checking access to %s %s under scope %s", request.Method, request.URL.Path, scope)

		// Check is the scope contains the method and path
		for _, resource := range resources {

			// Search for the method
			for _, method := range resource.Methods {

				// If the method matches, check the resource path.
				if method == request.Method {

					// Search for the path
					for _, path := range resource.Paths {
						pathPattern := request.URL.Path

						if path != pathPattern && len(path) < len(pathPattern) {
							pathPattern = pathPattern[:len(path)]
						}

						// If the method and the path are part of this scope, make sure the
						// Token Info contains the required scope.
						if path == pathPattern {

							// Validate the required scope was included in the Token Info
							for _, tokenScope := range tokenInfo.Scopes {
								if tokenScope == scope {
									mlog.Debug("Access data is valid, request URL is valid. Return the validated request with no errors.")
									return nil
								}
							}
						}
					}
				}
			}
		}

		// Construct the scopes needed for the response in case of error
		scopes = append(scopes, scope)
	}

	writer.Header().Set("scope", strings.Join(scopes, " "))

	return ErrorUnauthorizedClient
}
