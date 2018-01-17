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

package utils

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetCorrelationId(t *testing.T) {
	c, err := GetCorrelationId(1, 29, 1)
	if err != nil {
		t.Error(err)
	}
	if c != 373 {
		t.Error(c)
	}
}

func TestGetNextGetCorrelationId(t *testing.T) {
	c, err := GetNextCorrelationId(373)
	if err != nil {
		t.Error(err)
	}
	if c != 374 {
		t.Error(c)
	}
}

func TestGetToken(t *testing.T) {
	tk := GetToken(1, 1, 16)
	if tk != 16842768 {
		t.Error(tk)
	}
}

func TestGetNextToken(t *testing.T) {
	tk := GetNextToken(16842783)
	if tk != 16842768 {
		t.Error(tk)
	}
}

func TestIsValidISO8601(t *testing.T) {

	Convey("Test IsValidISO8601", t, func() {
		So(IsValidISO8601("1997-07-16T19:20:30.45+01:00"), ShouldEqual, true)
		So(IsValidISO8601("1994-11-05T08:15:30-05:00"), ShouldEqual, true)
		So(IsValidISO8601("1994-11-05T13:15:30Z"), ShouldEqual, true)
	})

	Convey("Test IsValidISO8601 - Invalid Dates", t, func() {
		So(IsValidISO8601("07-16-1997 13:15:30"), ShouldEqual, false)
		So(IsValidISO8601("1997/07/16"), ShouldEqual, false)
		So(IsValidISO8601("11 May 2014 Eastern European Summer Time"), ShouldEqual, false)
		So(IsValidISO8601("Sun, 11 May 2014 23:11:51 EEST"), ShouldEqual, false)
		So(IsValidISO8601("Sun May 11 23:11:51 EEST 2014"), ShouldEqual, false)
	})
}

func TestGetTimeFromISO8601(t *testing.T) {
	Convey("Test GetTimeFromISO8601", t, func() {
		t, err := GetTimeFromISO8601("1997-07-16T19:20:30.45+01:00")

		So(t, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})

	Convey("Test GetTimeFromISO8601 - Invalid Dates", t, func() {
		_, err := GetTimeFromISO8601("1997/07/16")

		So(err, ShouldNotBeNil)
	})
}
