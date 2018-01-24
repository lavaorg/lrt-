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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/lavaorg/lrt/x/management"
	"github.com/lavaorg/lrt/x/mlog"
	"github.com/pmylund/go-cache"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	CacheExpiration = 1 * time.Hour
	CachePurgeTime  = 30 * time.Minute
)

const (
	// Defines the access token grant types.
	AuthorizationCodeGrantType string = "authorization_code"
	PasswordGrantType          string = "password"
	ClientCredentialsGrantType string = "client_credentials"
)

// Defines the auth client interface.
type AuthClient interface {
	GetClientToken(clientId string, clientSecret string, scope string) (*Token, *management.Error)
	GetUserToken(clientId, clientSecret, username, password, scope string) (*Token, *management.Error)
	RevokeAccessToken(clientId, clientSecret, token string) *management.Error
	GetTokenInfo(token string) (*TokenInfo, *management.Error)
}

// Defines the  Auth client.
type NSAuthClient struct {
	BaseClient
	tokenCache *cache.Cache
}

// Defines the auth token type.
type Token struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	Type         string    `json:"token_type"`
	ExpiresIn    float64   `json:"expires_in"`
	Scope        string    `json:"scope"`
	CreatedAt    time.Time `json:"-"`
}

// Defines the type used to represent token information.
type TokenInfo struct {
	GrantType  string    `json:"grant_type,omitempty"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
	ExpiresIn  int32     `json:"expires_in,omitempty"`
	UserName   string    `json:"username"`
	CustomData string    `json:"custom_data,omitempty"`
	Scopes     []string  `json:"scope,omitempty"`
	ClientId   string    `json:"clientid"`
}

// Returns true if token is expired, false otherwise.
func (token Token) IsExpired() bool {
	return token.CreatedAt.Add(time.Duration(token.ExpiresIn) * time.Second).Before(time.Now())
}

// Returns true if token info is expired, false otherwise.
func (tokenInfo TokenInfo) IsExpired() bool {
	return tokenInfo.CreatedAt.Add(time.Duration(tokenInfo.ExpiresIn) * time.Second).Before(time.Now())
}

// Returns a new  Auth Service client.
func NewNSAuthClient(nsHostAndPort string) AuthClient {
	return NewNSAuthClientWithProtocol("http", nsHostAndPort)
}

// Returns a new  Auth Service client.
func NewNSAuthClientWithProtocol(protocol, hostAndPort string) AuthClient {
	authClient := &NSAuthClient{
		BaseClient: BaseClient{
			httpClient: management.NewHttpClient(),
			baseUrl:    fmt.Sprintf("%s://%s", protocol, hostAndPort),
		},
		tokenCache: cache.New(CacheExpiration, CachePurgeTime),
	}

	return authClient
}

// Returns the token associated with client credentials.
func (client *NSAuthClient) GetClientToken(clientId string, clientSecret string, scope string) (*Token, *management.Error) {
	mlog.Debug("GetClientToken")

	// Try to get the token from the cache.
	credentials := clientId + ":" + clientSecret
	cacheItem, found := client.tokenCache.Get(credentials)

	// If the item is found, we check if it has expire. If it did,
	// the request will be made again and item will be store in cache.
	// Otherwise, we return the value of the cache item.
	if found == true {
		mlog.Debug("Found internal token in cache.")
		token := cacheItem.(Token)

		if token.IsExpired() == false {
			mlog.Debug("Cache item still valid, reusing value.")
			return &token, nil
		} else {
			mlog.Debug("Cache item expired ")
		}
	}

	// Create request body.
	reqBody := url.Values{}
	reqBody.Set("grant_type", "client_credentials")
	reqBody.Set("scope", scope)

	// Create the request.
	request, _ := http.NewRequest("POST", client.baseUrl+"/oauth2/token", bytes.NewBufferString(reqBody.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Content-Length", strconv.Itoa(len(reqBody.Encode())))
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(credentials))))
	request.Header.Set("Connection", "close")

	// Send the request.
	response, err := client.httpClient.Do(request)

	if err != nil {
		mlog.Error("Error, failed to get client token from url %s with error: %s.", client.baseUrl, err.Error())
		return nil, management.GetInternalError(err.Error())
	}

	// Validate the response body and status code
	if response == nil || response.Body == nil {
		return nil, management.GetExternalError("Error, the response, or response body, is not valid.")
	}

	// Get the token from the response body
	respBody, _ := ioutil.ReadAll(response.Body)
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		mlog.Error("Error, failed to get client token with error: %s.", string(respBody))
		return nil, management.ErrorExternal
	}

	var token Token

	if err = json.Unmarshal(respBody, &token); err != nil {
		mlog.Error("Error, failed to unmarshal token from response with error: %s", err.Error())
		return nil, management.GetInternalError(err.Error())
	}

	// Note that response does not contains created at. We add to facilitate
	// validation when used from cache.
	token.CreatedAt = time.Now()

	// Store in cache using token expiration time.
	client.tokenCache.Set(credentials, token, time.Duration(token.ExpiresIn)*time.Second)

	return &token, nil
}

// Returns the token associated with user credentials.
func (client *NSAuthClient) GetUserToken(clientId, clientSecret, username, password, scope string) (*Token, *management.Error) {
	mlog.Debug("GetUserToken")

	// Note that we do not cache user token.

	credentials := clientId + ":" + clientSecret
	reqBody := url.Values{}
	reqBody.Set("grant_type", "password")
	reqBody.Set("scope", scope)
	reqBody.Set("username", username)
	reqBody.Set("password", password)

	request, _ := http.NewRequest("POST", client.baseUrl+"/oauth2/token", bytes.NewBufferString(reqBody.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Content-Length", strconv.Itoa(len(reqBody.Encode())))
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(credentials))))
	request.Header.Set("Connection", "close")

	// Send the request.
	response, err := client.httpClient.Do(request)

	if err != nil {
		mlog.Error("Error, failed to get user token from url %s with error: %s.", client.baseUrl, err.Error())
		return nil, management.GetInternalError(err.Error())
	}

	// Validate the response body and status code
	if response == nil || response.Body == nil {
		return nil, management.GetExternalError("Error, the response, or response body, is not valid.")
	}

	// Get the token from the response body
	respBody, _ := ioutil.ReadAll(response.Body)
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		mlog.Error("Error, failed to get user token with error: %s.", string(respBody))
		return nil, management.ErrorExternal
	}

	var token Token

	if err = json.Unmarshal(respBody, &token); err != nil {
		mlog.Error("Error, failed to unmarshal token from response with error: %s", err.Error())
		return nil, management.GetInternalError(err.Error())
	}

	return &token, nil
}

// Revokes, or removes, the specified access token.
func (client *NSAuthClient) RevokeAccessToken(clientId, clientSecret, token string) *management.Error {
	mlog.Debug("RevokeAccessToken")

	reqBody := url.Values{}
	reqBody.Set("token", token)
	reqBody.Set("token_type_hint", "access_token")

	credentials := clientId + ":" + clientSecret
	request, _ := http.NewRequest("POST", client.baseUrl+"/oauth2/revoke", bytes.NewBufferString(reqBody.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Content-Length", strconv.Itoa(len(reqBody.Encode())))
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(credentials))))
	request.Header.Set("Connection", "close")

	// Send the request.
	response, err := client.httpClient.Do(request)

	if err != nil {
		mlog.Error("Error, failed to get user token from url %s with error: %s.", client.baseUrl, err.Error())
		return management.GetInternalError(err.Error())
	}

	// Validate the response body and status code
	if response == nil || response.Body == nil {
		return management.GetExternalError("Error, the response, or response body, is not valid.")
	}

	// Validate http status.
	if response.StatusCode != http.StatusOK {
		// Get the error from the response body
		respBody, _ := ioutil.ReadAll(response.Body)
		response.Body.Close()

		mlog.Error("Error, failed to revoke token with error: %s.", string(respBody))
		return management.ErrorExternal
	}

	return nil
}

// Returns information associated with the specified token.
func (client *NSAuthClient) GetTokenInfo(token string) (*TokenInfo, *management.Error) {
	mlog.Debug("GetTokenInfo")

	url := client.baseUrl + "/oauth2/token/info?access_token=" + token

	// Request token information.
	body, err := client.executeHttpMethod("GET", url, "", nil)

	if err != nil {
		mlog.Error("Error, failed to execute http request for url %s with error: %s", url, err.Description)
		return nil, err
	}

	tokenInfo := &TokenInfo{}

	if goErr := json.Unmarshal(body, tokenInfo); goErr != nil {
		mlog.Error("Error, failed to unmarshal token information with error: %s", goErr.Error())
		return nil, management.GetInternalError(goErr.Error())
	}

	return tokenInfo, nil
}
