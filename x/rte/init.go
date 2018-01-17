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

package rte

import (
	"fmt"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/rte/config"
	"github.com/verizonlabs/northstar/pkg/rte/events"
)

func InitRTE(rteType string) error {
	eventsHander, err := events.NewEventsHandler(rteType)
	if err != nil {
		return err
	}

	go eventsHander.Start()

	if err := management.Listen(":0"); err != nil {
		mlog.Error("Error starting web server: %v", err)
		return err
	}

	return nil
}

func InitManagement() error {
	if err := management.Listen(fmt.Sprintf(":%d", config.WebPort)); err != nil {
		mlog.Error("Error starting management endpoint: %v", err)
		return err
	}

	return nil
}
