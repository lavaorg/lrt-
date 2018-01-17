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

const (
	// state of a newly created User who has not been confirmed.
	USER_STATE_NEW string = "New"

	// state of an User who has been confirmed sucessfully.
	USER_STATE_ACTIVE = "Active"
)

// Defines the type used to represent Account resource.
type Account struct {
	// Resource Identity
	Id          string `json:"id,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Version     string `json:"version"`
	CreatedOn   string `json:"createdon,omitempty"`
	LastUpdated string `json:"lastupdated,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	CustomDataId string `json:"customdataid,omitempty"`
}

type User struct {
	//  Resource Identity
	Id          string `json:"id,omitempty"`
	Kind        string `json:"kind"`
	Version     string `json:"version"`
	CreatedOn   string `json:"createdon,omitempty"`
	LastUpdated string `json:"lastupdated,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// Resource Foreign Identity
	ForeignId string   `json:"foreignid,omitempty"`
	Tags      []string `json:"tagids,omitempty"`

	// User Resource. Note that this might not
	// be the complete property list. Only the onces needed
	// by dakota.
	CredentialsId string `json:"credentialsid,omitempty"`
	State         string `json:"state,omitempty"`
	DisplayName   string `json:"displayname,omitempty"`
	Email         string `json:"email,omitempty"`
	ImageId       string `json:"imageid,omitempty"`
}
