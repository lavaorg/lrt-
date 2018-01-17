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

package msgq

import (
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq"
)

type MockConsumer struct{}

func (m MockConsumer) Receive() chan *msgq.ConsumerEvent {
	mlog.Info("Recieving channel")
	return nil
}

func (m MockConsumer) SetAckOffset(ofs int64) error {
	mlog.Info("Setting ack offset by %v", ofs)
	return nil
}

func (m MockConsumer) GetPartitionNum() int32 {
	mlog.Info("Returning partition number")
	return 3
}

func (m MockConsumer) Close() error {
	mlog.Info("Closing consumer channel")
	return nil
}
