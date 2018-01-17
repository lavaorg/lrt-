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

package crypto

import (
	"fmt"
	"testing"
)

func TestAesGcmEncryption(t *testing.T) {
	key := []byte("AES256Key-32Characters1234567890")
	nonce := []byte("12bytenonce!")
	plainText := []byte("Plain text message to be encrypted. But this message lenght should not be longer than 94 bytes. ")
	fmt.Println("Plain text", string(plainText))
	cipherText, err := Encrypt(plainText, nonce, key)
	if err != nil {
		t.Error(err)
	}

	decrtypedText, err := Decrypt(cipherText, nonce, key)
	if err != nil {
		t.Error(err)
	}

	if decrtypedText != plainText {
		t.Error("Decrypted text is not same as plain text")
	}
}
