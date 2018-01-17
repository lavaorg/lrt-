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

package util

import (
	"errors"
	"github.com/satori/go.uuid"
	"net/url"
	"os"
	"os/exec"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/b64"
	"github.com/verizonlabs/northstar/pkg/file"
)

const (
	TMP_DIRECTORY = "/tmp"
)

func ExecCmd(cmd string, args string) error {
	mlog.Debug("CMD %s with args %s: ", cmd, args)

	if _, err := os.Stat(cmd); err != nil {
		return err
	}

	if err := exec.Command(cmd, args).Start(); err != nil {
		mlog.Error("Failed to start command: %v", err)
		return err
	}

	return nil
}

func GetSnippetCode(fullUrl string, code string) (string, error) {
	if fullUrl == "" || code == "" {
		return "", errors.New("URL or code empty")
	}

	parsed, err := url.Parse(fullUrl)
	if err != nil {
		mlog.Error("Failed to parse source %s", fullUrl)
		return "", err
	}

	switch parsed.Scheme {
	case "base64":
		mlog.Debug("Base64 source detected")
		return b64.DecodeBase64ToString(code)
	case "s3":
		mlog.Debug("S3 source detected")
		return "", errors.New("S3 schema not supported!")
	case "http":
		mlog.Debug("HTTP schema detected")
		fileName := TMP_DIRECTORY + "/" + "rte-" + uuid.NewV4().String()
		err := file.DownloadFromHttpToLocal(parsed.Host, parsed.Path, fileName)
		if err != nil {
			mlog.Error("Failed to download file: %v", err)
			return "", err
		}
		return file.ReadFile(fileName)
	default:
		return "", errors.New("Unknow schema detected: " + parsed.Scheme)
	}

	return "", nil
}
