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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"os"
)

// Defines the client interface.
type Client interface {
	QueryAccounts(query Query) ([]Account, *management.Error)
	QueryUsers(query Query) ([]User, *management.Error)
}

// Defines the client.
type AccountsClient struct {
	BaseClient
	authClient   AuthClient
	clientId     string
	clientSecret string
}

// Returns a new  client.
func NewAccountsClient(accountsHostAndPort string, clientId string, clientSecret string) (Client, error) {
	mlog.Info("NewAccountsClient")

	// If no host and port provided, attempt to get default value.
	if accountsHostAndPort == "" {
		mlog.Info("Getting Accounts Host and Port from environment variable.")

		if accountsHostAndPort = os.Getenv("ACCOUNTS_HOST_PORT"); accountsHostAndPort == "" {
			mlog.Error("Error, failed to get Accounts Host and Port.")
			return nil, errors.New("Error, Accounts Host and Port environment variable not found or invalid")
		}
	}

	// Create the Accounts User Client.
	accountsClient := &AccountsClient{
		BaseClient: BaseClient{
			httpClient: management.NewHttpClient(),
			baseUrl:    fmt.Sprintf("http://%s", accountsHostAndPort),
		},
		authClient:   NewNSAuthClient(accountsHostAndPort),
		clientId:     clientId,
		clientSecret: clientSecret,
	}

	return accountsClient, nil
}

// Returns the account that meet the specified query.
func (client *AccountsClient) QueryAccounts(query Query) ([]Account, *management.Error) {
	mlog.Debug("QueryAccounts: query:%#v", query)

	url := client.baseUrl + BASE_PATH + "accounts"
	body, err := client.executeHttpMethod("GET", url, "", query)

	if err != nil {
		mlog.Error("Error, failed to get accounts with error: %s", url, err.Description)
		return nil, err
	}

	accounts := make([]Account, 0)
	if err := json.Unmarshal(body, &accounts); err != nil {
		mlog.Error("Error, failed to unmarshal response to accounts with error: %s.", err.Error())
		return nil, management.ErrorInternal
	}

	return accounts, nil
}

// Returns the users that meet the specified query.
func (client *AccountsClient) QueryUsers(query Query) ([]User, *management.Error) {
	mlog.Debug("QueryUsers: query:%#v", query)

	url := client.baseUrl + BASE_PATH + "users"
	body, err := client.executeHttpMethod("GET", url, "", query)

	if err != nil {
		mlog.Error("Error, failed to get users with error: %s", url, err.Description)
		return nil, err
	}

	users := make([]User, 0)
	if err := json.Unmarshal(body, &users); err != nil {
		mlog.Error("Error, failed to unmarshal response to users with error: %s.", err.Error())
		return nil, management.ErrorInternal
	}

	return users, nil
}
