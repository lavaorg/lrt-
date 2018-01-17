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

package file

import (
	"io/ioutil"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

func ReadFile(file string) (string, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func DownloadFromHttpToLocal(host string, path string, dest string) error {
	mlog.Info("Downloading file %s from %s to %s", path, host, dest)

	resp, err := management.Get("http://"+host, path)
	if err != nil {
		return err
	}

	wErr := ioutil.WriteFile(dest, resp, 0644)
	if wErr != nil {
		return wErr
	}

	return nil
}
