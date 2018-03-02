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
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"net/http"
	"testing"
)

func TestEngine(t *testing.T) {

	convey.Convey("Test Management Engine", t, func() {

		convey.So(Engine(), convey.ShouldNotBeNil)

		convey.Convey("Test standard GET error", func() {
			Engine().GET("/test", func(context *gin.Context) {
				context.JSON(http.StatusBadRequest, Error{
					Id:          "BAD_REQUEST",
					Description: "a bad request",
				})
			})
			go Listen(":8987")

			_, merr := Get("http://localhost:8987", "/test")
			convey.So(merr, convey.ShouldNotBeNil)
			convey.So(merr.Id, convey.ShouldEqual, "BAD_REQUEST")
			convey.So(merr.Description, convey.ShouldEqual, "a bad request")
		})

		convey.Convey("Test nonstandard GET error", func() {
			Engine().GET("/test1", func(context *gin.Context) {
				context.JSON(http.StatusBadRequest, gin.H{"status": "bad"})
			})
			go Listen(":8988")

			_, uerr := Get("http://localhost:8988", "/test1")
			convey.So(uerr, convey.ShouldNotBeNil)
			convey.So(uerr.Id, convey.ShouldEqual, "Bad Request")
			convey.So(uerr.Description, convey.ShouldEqual, "{\"status\":\"bad\"}\n")
		})

		convey.Convey("Test POST error", func() {
			Engine().POST("/test2", func(context *gin.Context) {
				var merr Error
				if context.Bind(&merr) {
					context.JSON(http.StatusBadRequest, Error{
						Id:          "GOOD_REQUEST",
						Description: "a good request",
					})
				} else {
					context.JSON(http.StatusBadRequest, Error{
						Id:          "BAD_REQUEST",
						Description: "a bad request",
					})
				}
			})
			go Listen(":8989")

			_, merr := PostJSON("http://localhost:8989", "/test2", Error{Id: "TEST", Description: "TEST"})
			convey.So(merr, convey.ShouldNotBeNil)
			convey.So(merr.Id, convey.ShouldEqual, "GOOD_REQUEST")
			convey.So(merr.Description, convey.ShouldEqual, "a good request")
		})
	})
}
