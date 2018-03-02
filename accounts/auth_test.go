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

package accounts

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestFunctional(t *testing.T) {

	if os.Getenv("FUNCTIONAL_TESTING_ENABLED") == "" {
		t.Skip("Skipping test; $FUNCTIONAL_TESTING_ENABLED not set.")
	}

	hostport := os.Getenv("AUTH_USER_HOST_PORT")
	if hostport == "" {
		t.Fail()
	}

	clientId := "DakotaClientSimAppV2"
	clientSecret := "0b1400bd-e558-46a6-9a50-b3a282aa4ec3"

	Convey("Test Auth Client", t, func() {
		authClient := NewNSAuthClient(hostport)

		// Get token
		token, serviceError := authClient.GetClientToken(clientId, clientSecret, "ts.account ts.account.wo")

		fmt.Println(token)

		So(serviceError, ShouldBeNil)
		So(token, ShouldNotBeNil)
		So(token.AccessToken, ShouldNotBeBlank)

		// Get token information
		tokenInfo, serviceError := authClient.GetTokenInfo(token.AccessToken)

		So(serviceError, ShouldBeNil)
		So(tokenInfo, ShouldNotBeNil)
		So(tokenInfo.GrantType, ShouldEqual, api.ClientCredentialsGrantType)
		So(tokenInfo.ClientId, ShouldEqual, clientId)
		So(tokenInfo.Scopes, ShouldContain, "ts.account")
		So(tokenInfo.Scopes, ShouldContain, "ts.account.wo")
	})
}
