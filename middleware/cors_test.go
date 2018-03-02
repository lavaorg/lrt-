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
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCors(t *testing.T) {

	Convey("Test Cors - Default Access Control", t, func() {
		server := gin.New()
		server.Use(Cors(AccessControl{}))
		server.OPTIONS("/cors", func(context *gin.Context) {
			context.String(http.StatusOK, http.StatusText(http.StatusOK))
		})

		request, _ := http.NewRequest("OPTIONS", "/cors", nil)
		writer := httptest.NewRecorder()

		server.ServeHTTP(writer, request)

		// Validate default values
		So(writer.Header().Get("Access-Control-Allow-Origin"), ShouldEqual, "")
		So(writer.Header().Get("Access-Control-Expose-Headers"), ShouldEqual, "")
		So(writer.Header().Get("Access-Control-Allow-Credentials"), ShouldEqual, "")
		So(writer.Header().Get("Access-Control-Allow-Headers"), ShouldEqual, "Origin,Accept,Content-Type,Authorization")
		So(writer.Header().Get("Access-Control-Allow-Methods"), ShouldEqual, "GET,POST,PUT,DELETE,PATCH,HEAD")
		So(writer.Header().Get("Access-Control-Max-Age"), ShouldEqual, "")
	})

	Convey("Test Cors - Non Access Control", t, func() {
		accessControl := AccessControl{
			AllowOrigins:     []string{"*"},
			AllowCredentials: true,
			AllowMethods:     []string{"GET", "POST"},
			AllowHeaders:     []string{"Content-Type", "Authorization"},
			ExposeHeaders:    []string{"ETag"},
			MaxAge:           time.Duration(30),
		}

		server := gin.New()
		server.Use(Cors(accessControl))
		server.OPTIONS("/cors", func(context *gin.Context) {
			context.String(http.StatusOK, http.StatusText(http.StatusOK))
		})

		request, _ := http.NewRequest("OPTIONS", "/cors", nil)
		writer := httptest.NewRecorder()

		server.ServeHTTP(writer, request)

		// Validate default values
		So(writer.Header().Get("Access-Control-Allow-Origin"), ShouldEqual, "*")
		So(writer.Header().Get("Access-Control-Expose-Headers"), ShouldEqual, "ETag")
		So(writer.Header().Get("Access-Control-Allow-Credentials"), ShouldEqual, "true")
		So(writer.Header().Get("Access-Control-Allow-Headers"), ShouldEqual, "Content-Type,Authorization")
		So(writer.Header().Get("Access-Control-Allow-Methods"), ShouldEqual, "GET,POST")
		So(writer.Header().Get("Access-Control-Max-Age"), ShouldEqual, "30")
	})
}
