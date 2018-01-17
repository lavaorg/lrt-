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

package stats

import "github.com/verizonlabs/northstar/pkg/stats"

var (
	RTE                    = stats.New("rte")
	RunSnippet             = RTE.NewCounter("RunSnippet")
	OnReceiveMessage       = RTE.NewCounter("OnReceiveMessage")
	StoreInvocationOutput  = RTE.NewCounter("StoreInvocationOutput")
	UpdateInvocationStatus = RTE.NewCounter("UpdateInvocationStatus")
	SnippetOutputCallback  = RTE.NewCounter("SnippetOutputCallback")
	SnippetOutput          = RTE.NewCounter("SnippetOutput")
	SnippetStart           = RTE.NewCounter("SnippetStart")
	SnippetStop            = RTE.NewCounter("SnippetStop")

	ErrRunSnippet             = RTE.NewCounter("ErrRunSnippet")
	ErrOnReceiveMessage       = RTE.NewCounter("ErrOnReceiveMessage")
	ErrStoreInvocationOutput  = RTE.NewCounter("ErrStoreInvocationOutput")
	ErrUpdateInvocationStatus = RTE.NewCounter("ErrUpdateInvocationStatus")
	ErrSnippetOutputCallback  = RTE.NewCounter("ErrSnippetOutputCallback")
	ErrSnippetStart           = RTE.NewCounter("ErrSnippetStart")
	ErrSnippetStop            = RTE.NewCounter("ErrSnippetStop")
	ErrSnippetOutput          = RTE.NewCounter("ErrSnippetOutput")
)
