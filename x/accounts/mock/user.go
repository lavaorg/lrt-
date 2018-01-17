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

package mock

import (
	gomock "github.com/golang/mock/gomock"
	. "github.com/lavaorg/lrt/x/accounts"
	management "github.com/lavaorg/lrt/x/management"
)

// Mock of Client interface
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *_MockClientRecorder
}

// Recorder for MockClient (not exported)
type _MockClientRecorder struct {
	mock *MockClient
}

func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &_MockClientRecorder{mock}
	return mock
}

func (_m *MockClient) EXPECT() *_MockClientRecorder {
	return _m.recorder
}

func (_mr *_MockClientRecorder) QueryAccounts(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "QueryAccounts", arg0)
}

func (_m *MockClient) QueryUsers(query Query) ([]User, *management.Error) {
	ret := _m.ctrl.Call(_m, "QueryUsers", query)
	ret0, _ := ret[0].([]User)
	ret1, _ := ret[1].(*management.Error)
	return ret0, ret1
}

func (_mr *_MockClientRecorder) QueryUsers(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "QueryUsers", arg0)
}

func (_m *MockClient) QueryPlaces(query Query) ([]ut.Place, *management.Error) {
	ret := _m.ctrl.Call(_m, "QueryPlaces", query)
	ret0, _ := ret[0].([]ut.Place)
	ret1, _ := ret[1].(*management.Error)
	return ret0, ret1
}

func (_mr *_MockClientRecorder) QueryPlaces(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "QueryPlaces", arg0)
}
