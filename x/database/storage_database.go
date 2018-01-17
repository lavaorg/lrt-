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

package database

import (
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

type Database struct {
	storage
	seeds        []string
	cluster      *gocql.ClusterConfig
	instanceLock sync.Mutex
	instance     *gocql.Session
}

func NewDatabase(rtype reflect.Type, seeds ...string) (Storage, *management.Error) {
	return new(Database).init(rtype, seeds...)
}

func (this *Database) init(rtype reflect.Type, seeds ...string) (Storage, *management.Error) {
	if merr := this.parse(rtype); merr != nil {
		return nil, merr
	}

	this.rtype = rtype
	this.seeds = seeds
	this.cluster = gocql.NewCluster(seeds...)
	this.cluster.Keyspace = this.keyspace
	this.cluster.Consistency = gocql.LocalQuorum
	this.cluster.Timeout = 60 * time.Second
	this.cluster.ProtoVersion = 3
	this.cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 5}
	return this, nil
}

func (this *Database) session() (*gocql.Session, *management.Error) {
	if this.instance == nil || this.instance.Closed() {
		this.instanceLock.Lock()
		defer this.instanceLock.Unlock()
		if this.instance == nil || this.instance.Closed() {
			var err error
			if this.instance, err = this.cluster.CreateSession(); err != nil {
				return nil, management.NewError(http.StatusInternalServerError, "session_failed", "database create session failed, %v", err)
			}
		}
	}
	return this.instance, nil
}

// Query
// Select a single row based on the given where clauses, e.g.,
// SELECT * FROM keyspace.table WHERE col1=val1, where the binding
// is represented as string/interface{} (i.e. see Binding) pair to
// this function.
//
// Example
//    Query and single "Row" type where the "column1" is 5,
//
//      session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
//      result, _ := session.Query( Bind("column1",5) )
//      row := result.(Row)
//
func (this *Database) Query(clauses ...Binding) (interface{}, *management.Error) {
	mlog.Debug("Database.Query")

	// establish session
	local, merr := this.session()
	if merr != nil {
		return nil, merr
	}

	// build select statement
	builder := Select(this.keyspace, this.family)
	builder.Wheres(clauses...)

	result := reflect.New(this.rtype).Elem()
	for i := 0; i < result.NumField(); i++ {
		f := result.Field(i)
		a := this.fieldToColumn[this.rtype.Field(i).Name]
		builder.Value(a.column, f.Addr().Interface())
	}

	// **TODO: this should be made optional
	builder.AllowFiltering()

	// execute select
	if err := builder.Scan(local); err != nil {
		return nil, management.NewError(http.StatusBadRequest, "select_failed", err.Error())
	}

	// return result
	return result.Interface(), nil
}

// QueryMany
// Select multiple rows, limited by the given limit (or unlimited if the
// given limit is zero), based on the given where clauses
//
// Example
//   Query many "Row"s where the "column2", is "green"
//
//     session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
//     results, _ := session.Query( Bind("column2", "green") )
//     row := results[0].(Row)
//
func (this *Database) QueryMany(limit int, clauses ...Binding) ([]interface{}, *management.Error) {
	mlog.Debug("Database.QueryMany")

	// establish session
	local, merr := this.session()
	if merr != nil {
		return nil, merr
	}

	// build select statement
	builder := Select(this.keyspace, this.family)
	builder.Wheres(clauses...)
	builder.Limit(limit)

	result := reflect.New(this.rtype).Elem()
	values := make([]Binding, 0)
	for i := 0; i < result.NumField(); i++ {
		f := result.Field(i)
		a := this.fieldToColumn[this.rtype.Field(i).Name]
		values = append(values, Binding{name: a.column, value: f.Addr().Interface()})
	}
	builder.Values(values...)

	// **TODO: this should be made optional
	builder.AllowFiltering()

	// execute select
	results := make([]interface{}, 0)
	iterator := builder.Iter(local)
	for builder.Next(iterator) {
		results = append(results, result.Interface())

		result = reflect.New(this.rtype).Elem()
		values = make([]Binding, 0)
		for i := 0; i < result.NumField(); i++ {
			f := result.Field(i)
			a := this.fieldToColumn[this.rtype.Field(i).Name]
			values = append(values, Binding{name: a.column, value: f.Addr().Interface()})
		}
		builder.Values(values...)
	}

	// return result
	return results, nil
}

// InsertMany
// Insert as a batch the list of given data according to the dataMask. If
// data contains only one value, then a single transactional insert is
// executed (i.e., if-not-exists). Insert will insert all of the values
// defined by the struct, which means types like UUIDs must be initialized
// to an equivalent nil value if they are not set already.
// (e.g., NilUUID = "00000000-0000-0000-0000-000000000000"
//
// Example,
//   Insert a single or batch set of "Row",
//
//     session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
//     rows := []Row{Row{ column1: 123 }, Row{ column1: 124 }}
//     _ := session.Insert( rows )
//
func (this *Database) InsertMany(data ...interface{}) *management.Error {
	mlog.Debug("Database.InsertMany")

	// preconditions
	if len(data) == 0 {
		return management.NewError(http.StatusBadRequest, "insert_failed", "data set empty")
	}

	for _, item := range data {
		rtype := reflect.TypeOf(item)
		if this.rtype.Name() != rtype.Name() {
			return management.NewError(http.StatusInternalServerError, "insert_failed", "data set type mismatch")
		}
	}

	successchan := make(chan int)
	errchan := make(chan *management.Error)
	// determine if batch
	batch := len(data) > 1
	for _, item := range data {
		go this.insert(item, successchan, errchan, batch)
	}
	defer close(successchan)
	defer close(errchan)
	for index := 0; index < len(data); index++ {
		select {
		case <-successchan:
			continue
		case e := <-errchan:
			return e
		}
	}
	return nil
}

//privae function to insert single record which will be spawned as go routies by InsertMany
//if operation is successful it will write to succes channel schan otherwise will write to error channel echan
func (this *Database) insert(data interface{}, schan chan int, echan chan *management.Error, batch bool) {
	defer func() {
		if r := recover(); r != nil {
			echan <- management.NewError(http.StatusBadRequest, "insert_failed", fmt.Sprintf("%v", r))
		}
	}()
	// build insert statement
	builder := Insert(this.keyspace, this.family)
	if !batch {
		builder.IfNotExist()
	}

	// build insert params
	item := data
	vof := reflect.ValueOf(item)
	for i := 0; i < this.rtype.NumField(); i++ {
		f := vof.Field(i)
		a := this.fieldToColumn[this.rtype.Field(i).Name]
		builder.Param(a.column, f.Interface())
	}
	local, merr := this.session()
	if merr != nil {
		echan <- merr
		return
	}
	// execute
	success, err := builder.Exec(local)
	if err != nil {
		echan <- management.NewError(http.StatusBadRequest, "insert_failed", err.Error())
		return
	}
	if !success {
		echan <- management.NewError(http.StatusBadRequest, "exists", "primary key already exists")
		return
	}
	schan <- 0
}

// Update
// Update the given data according to the dataMask. The dataMask is a set
// of columns (attributes) indicating which is being updated.
//
// Example:
//   This pseudo-code would update the row with the primary key, "id" of 456.
//   Because the mask only includes "column1" and "column2", the "id" and
//   "column2" wont be affected (even though they are set in the struct).
//
//     row := Row{ id: 456, column1: "hello", column2: 123, column3: "yay" }
//     var mask Mask
//     mask["column1"] = struct{}{}
//     mask["column3"] = struct{}{}
//     session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
//     result, err := session.Update( row, mask, Bind("id",456), nil)
//
func (this *Database) Update(data interface{}, dataMask Mask, wheres []Binding, ifs []Binding) *management.Error {
	mlog.Debug("Database.Update")

	// preconditions
	rtype := reflect.TypeOf(data)
	if this.rtype.Name() != rtype.Name() {
		return management.NewError(http.StatusInternalServerError, "update_failed", "data set type mismatch")
	}

	// establish session
	local, merr := this.session()
	if merr != nil {
		return merr
	}

	// build insert statement
	builder := Update(this.keyspace, this.family)
	builder.Wheres(wheres...)
	builder.Ifs(ifs...)

	// get next item
	vof := reflect.ValueOf(data)
	for i := 0; i < this.rtype.NumField(); i++ {
		f := vof.Field(i)
		a := this.fieldToColumn[this.rtype.Field(i).Name]

		// ignore update on column if marked, "readonly", "partitionkey" or "clusterkey"
		// also ignore if the data mask for that column isnt set
		exists := true
		if a.dtype == DTypeReadOnly || a.dtype == DTypePartitionKey || a.dtype == DTypeClusterKey {
			exists = false
		} else if dataMask != nil {
			_, exists = dataMask[a.column]
		}
		if exists {
			builder.Param(a.column, f.Interface())
		}
	}

	// execute
	success, err := builder.Exec(local)
	if err != nil {
		return management.NewError(http.StatusBadRequest, "update_failed", err.Error())
	}
	if !success {
		return management.NewError(http.StatusBadRequest, "mismatch", "if clause failed comparison test")
	}

	// return ok
	return nil
}

// Delete
// Delete the data based on the given where clauses.
//
// Example
//   Remove the "Row" where "column1" is 5
//
//     session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
//     result, err := session.Delete( Bind("column1",5) )
//
func (this *Database) Delete(clauses ...Binding) *management.Error {
	mlog.Debug("Database.Delete")

	// establish session
	local, merr := this.session()
	if merr != nil {
		return merr
	}

	// build delete statement
	builder := Delete(this.keyspace, this.family)
	builder.Wheres(clauses...)

	// execute delete
	if _, err := builder.Exec(local); err != nil {
		return management.NewError(http.StatusBadRequest, "delete_failed", err.Error())
	}
	return nil
}
