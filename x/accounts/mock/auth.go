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

// Mock of AuthClient interface
type MockAuthClient struct {
	ctrl     *gomock.Controller
	recorder *_MockAuthClientRecorder
}

// Recorder for MockAuthClient (not exported)
type _MockAuthClientRecorder struct {
	mock *MockAuthClient
}

func NewMockAuthClient(ctrl *gomock.Controller) *MockAuthClient {
	mock := &MockAuthClient{ctrl: ctrl}
	mock.recorder = &_MockAuthClientRecorder{mock}
	return mock
}

func (_m *MockAuthClient) EXPECT() *_MockAuthClientRecorder {
	return _m.recorder
}

func (_m *MockAuthClient) GetClientToken(clientId string, clientSecret string, scope string) (*Token, *management.Error) {
	ret := _m.ctrl.Call(_m, "GetClientToken", clientId, clientSecret, scope)
	ret0, _ := ret[0].(*Token)
	ret1, _ := ret[1].(*management.Error)
	return ret0, ret1
}

func (_mr *_MockAuthClientRecorder) GetClientToken(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetClientToken", arg0, arg1, arg2)
}

func (_m *MockAuthClient) GetTokenInfo(token string) (*TokenInfo, *management.Error) {
	ret := _m.ctrl.Call(_m, "GetTokenInfo", token)
	ret0, _ := ret[0].(*TokenInfo)
	ret1, _ := ret[1].(*management.Error)
	return ret0, ret1
}

func (_mr *_MockAuthClientRecorder) GetTokenInfo(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetTokenInfo", arg0)
}
