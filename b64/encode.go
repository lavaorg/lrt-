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

package b64

import (
	b64 "encoding/base64"
	"io/ioutil"
)

func EncodeFileToString(file string) (string, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}

	encoded := b64.StdEncoding.EncodeToString(b)
	return encoded, nil
}

func DecodeBase64ToString(input string) (string, error) {
	data, err := b64.StdEncoding.DecodeString(input)
	return string(data), err
}
