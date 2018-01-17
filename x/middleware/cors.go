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
	"github.com/gin-gonic/gin"
	"net/http"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"strconv"
	"strings"
	"time"
)

var (
	// Defines the default header field names that can be used during a service request.
	defaultAllowHeaders = []string{"Origin", "Accept", "Content-Type", "Authorization"}

	// Defines the default methods that can be used during a service request.
	defaultAllowMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"}
)

// Defines the type used to control service Cross-Origin-Resource-Sharing (CORS) support.
// See http://www.w3.org/TR/cors/#access-control-allow-methods-response-header for more
// information.
type AccessControl struct {
	AllowOrigins     []string
	AllowCredentials bool
	AllowMethods     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	MaxAge           time.Duration
}

// Returns the GO-GIN Middleware used to setup service CORS support.
func Cors(accessControl AccessControl) gin.HandlerFunc {
	mlog.Info("Cors")

	// Setup alloww headers
	if accessControl.AllowHeaders == nil {
		mlog.Debug("Using default allow headers: %v", defaultAllowHeaders)
		accessControl.AllowHeaders = defaultAllowHeaders
	}

	// Setup allow methods
	if accessControl.AllowMethods == nil {
		mlog.Debug("Using default allow methods: %v", defaultAllowMethods)
		accessControl.AllowMethods = defaultAllowMethods
	}

	// Return the middleware
	return func(context *gin.Context) {
		mlog.Info("CorsMiddleware")
		request := context.Request
		writer := context.Writer

		// Set the allow origin header
		if len(accessControl.AllowOrigins) > 0 {
			writer.Header().Set("Access-Control-Allow-Origin", strings.Join(accessControl.AllowOrigins, " "))
		}

		// Set the allow expose headers
		if len(accessControl.ExposeHeaders) > 0 {
			writer.Header().Set("Access-Control-Expose-Headers", strings.Join(accessControl.ExposeHeaders, " "))
		}

		// Set the allow credentials header
		if accessControl.AllowCredentials == true {
			writer.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		if request.Method == "OPTIONS" {
			mlog.Info("Handling OPTIONS request.")

			// Set the allow headers and method
			writer.Header().Set("Access-Control-Allow-Headers", strings.Join(accessControl.AllowHeaders, ","))
			writer.Header().Set("Access-Control-Allow-Methods", strings.Join(accessControl.AllowMethods, ","))

			// Set the max age
			if accessControl.MaxAge > time.Duration(0) {
				writer.Header().Set("Access-Control-Max-Age", strconv.FormatInt(int64(accessControl.MaxAge), 10))
			}

			context.AbortWithStatus(http.StatusOK)
		} else {
			context.Next()
		}
	}
}
