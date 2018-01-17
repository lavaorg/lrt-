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
	"github.com/verizonlabs/northstar/pkg/management"
	"net/http"
	"reflect"
	"strconv"
)

// Memory
type Memory struct {
	storage
	family map[string]map[string]interface{} // col-name x hash x field-value
}

func NewMemory(rtype reflect.Type) (Storage, *management.Error) {
	return new(Memory).init(rtype)
}

func (this *Memory) init(rtype reflect.Type) (Storage, *management.Error) {
	if merr := this.parse(rtype); merr != nil {
		return nil, merr
	}
	this.rtype = rtype
	this.family = make(map[string]map[string]interface{})
	return this, nil
}

func (this *Memory) getHash(target interface{}) string {

	var result string
	tovalof := reflect.ValueOf(target)
	rtype := this.rtype
	fieldToColumn := this.fieldToColumn
	for i := 0; i < rtype.NumField(); i++ {

		f := rtype.Field(i)
		if a, aexists := fieldToColumn[f.Name]; aexists {

			if a.dtype == DTypePartitionKey || a.dtype == DTypeClusterKey {
				tofield := tovalof.Field(i)
				switch tofield.Kind() {
				case reflect.String:
					result += tofield.String()
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					result += strconv.FormatInt(tofield.Int(), 10)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					result += strconv.FormatUint(tofield.Uint(), 10)
				default:
					// ignore, panic("hash unsupported type")
				}
			}
		}
	}
	return result
}

func equals(lhs interface{}, rhs interface{}) bool {

	lhsValOf := reflect.ValueOf(lhs)
	rhsValOf := reflect.ValueOf(rhs)
	if lhsValOf.Kind() != rhsValOf.Kind() {
		return false
	}

	result := false
	switch lhsValOf.Kind() {
	case reflect.String:
		if result = (lhsValOf.Len() == rhsValOf.Len()); result {
			result = (lhs.(string) == rhs.(string))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result = (lhsValOf.Int() == rhsValOf.Int())
	case reflect.Float32, reflect.Float64:
		result = (lhsValOf.Float() == rhsValOf.Float())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		result = (lhsValOf.Uint() == rhsValOf.Uint())
	}

	return result
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
//      session, _ := NewMemory( reflect.TypeOf(Row{}) )
//      result, _ := session.Query( Bind("column1",5) )
//      row := result.(Row)
//
func (this *Memory) Query(clauses ...Binding) (interface{}, *management.Error) {
	results, err := this.QueryMany(0, clauses...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, management.NewError(http.StatusBadRequest, "not_found", "query did not find an entry by the given clause")
	}
	return results[0], nil
}

// QueryMany
// Select multiple rows, limited by the given limit (or unlimited if the
// given limit is zero), based on the given where clauses
//
// Example
//   Query many "Row"s where the "column2", is "green"
//
//     session, _ := NewMemory( reflect.TypeOf(Row{}) )
//     results, _ := session.Query( Bind("column2", "green") )
//     row := results[0].(Row)
//
func (this *Memory) QueryMany(limit int, clauses ...Binding) ([]interface{}, *management.Error) {

	// convert to hashes
	rank := make(map[string]int)
	for _, clause := range clauses {
		column := this.family[clause.name]
		for hash, value := range column {
			if equals(value, clause.value) {
				rank[hash] = rank[hash] + 1
			}
		}
	}

	// convert to struct-values
	var result []interface{}
	for hash, current := range rank {
		if current == len(clauses) {
			// create object from columns
			vof := reflect.New(this.rtype).Elem()
			for fname, attrib := range this.fieldToColumn {
				column := this.family[attrib.column]
				from := reflect.ValueOf(column[hash])
				vof.FieldByName(fname).Set(from)
			}
			// TODO: ** order by?
			result = append(result, vof.Interface())
		}
	}

	// not-found condition
	if len(result) == 0 {
		return nil, management.NewError(http.StatusNotFound, "not_found", "data not found")
	}

	// return ok
	return result, nil
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
//     session, _ := NewMemory( reflect.TypeOf(Row{}) )
//     rows := []Row{Row{ column1: 123 }, Row{ column1: 124 }}
//     _ := session.Insert( rows )
//
func (this *Memory) InsertMany(data ...interface{}) (err *management.Error) {

	// treat single inserts as transactions, just like the database version
	if len(data) == 1 {
		hash := this.getHash(data[0])
		for _, a := range this.fieldToColumn {
			if column, columnExists := this.family[a.column]; columnExists {
				if _, valueExists := column[hash]; valueExists {
					return management.NewError(http.StatusBadRequest, "exists", "primary key already exists")
				}
			}
		}
	}

	// inserts and updates are really the same
	for _, target := range data {
		if err = this.Update(target, nil, nil, nil); err != nil {
			return err
		}
	}

	// return ok
	return nil
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
//     session, _ := NewMemory( reflect.TypeOf(Row{}) )
//     result, err := session.Update( row, mask, Bind("id",456), nil)
//
func (this *Memory) Update(data interface{}, dataMask Mask, wheres []Binding, ifs []Binding) (err *management.Error) {

	// calculate hash
	hash := this.getHash(data)

	// family (index)
	tovalof := reflect.ValueOf(data)
	rtype := this.rtype
	fieldToColumn := this.fieldToColumn
	for i := 0; i < rtype.NumField(); i++ {

		f := rtype.Field(i)
		if a, aexists := fieldToColumn[f.Name]; aexists {

			// do not update all columns
			if dataMask != nil {
				if _, exists := dataMask[a.column]; !exists {
					continue
				}
			}

			// ** TODO: ifs, wheres?

			// set column for this hash
			// map[string]map[string]interface{} - col-name x hash x value
			column, columnExists := this.family[a.column]
			if !columnExists {
				column = make(map[string]interface{})
				this.family[a.column] = column
			}
			tofield := tovalof.Field(i)
			column[hash] = tofield.Interface()
		}
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
//     session, _ := NewMemory( reflect.TypeOf(Row{}) )
//     result, err := session.Delete( Bind("column1",5) )
//
func (this *Memory) Delete(clauses ...Binding) *management.Error {

	// find all objects
	all, err := this.QueryMany(0, clauses...)
	if err != nil {
		return err
	}

	// delete each column by hash for each object found by the given clause
	for _, each := range all {
		hash := this.getHash(each)
		for _, a := range this.fieldToColumn {
			if column, columnExists := this.family[a.column]; columnExists {
				if _, valueExists := column[hash]; valueExists {
					delete(column, hash)
				}
			}
		}
	}

	// return ok
	return nil
}
