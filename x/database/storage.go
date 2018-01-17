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
	"github.com/verizonlabs/northstar/pkg/mlog"
	"encoding/json"
	_ "github.com/gocql/gocql"
	"net/http"
	"reflect"
	"strings"
	_ "sync"
	"time"
)

const (
	StorageTypeMemory   StorageType = "memory"
	StorageTypeDatabase             = "database"
)

const (
	DTypePartitionKey = "partitionkey"
	DTypeClusterKey   = "clusterkey"
	DTypeReadOnly     = "readonly"
)

type StorageType string

type Mask map[string]struct{}

type Storage interface {

	// Unmarshal
	// Unmarshal the []byte data into a value (v) and mask (result), where the
	// parameters are,
	//   data    is the json data
	//   target  is the target type and value reference (i.e., writable)
	//
	// Note that empty strings are considered unset json values, this means that
	// the mask will not indicate empty strings
	Unmarshal(data []byte, target interface{}) (mask Mask, merr *management.Error)

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
	Query(clauses ...Binding) (interface{}, *management.Error)

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
	QueryMany(limit int, clauses ...Binding) ([]interface{}, *management.Error)

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
	InsertMany(data ...interface{}) *management.Error

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
	Update(data interface{}, dataMask Mask, wheres []Binding, ifs []Binding) *management.Error

	// Delete
	// Delete the data based on the given where clauses.
	//
	// Example
	//   Remove the "Row" where "column1" is 5
	//
	//     session, _ := NewDatabase( reflect.TypeOf(Row{}), "localhost" )
	//     result, err := session.Delete( Bind("column1",5) )
	//
	Delete(clauses ...Binding) *management.Error
}

type attribute struct {
	field   reflect.StructField
	column  string
	dtype   string
	jsontag string
}

type storage struct {
	rtype         reflect.Type
	keyspace      string
	family        string
	fieldToColumn map[string]attribute
	columnToField map[string]string
}

// parse
// (Private) parse the given type for 'db' tags, e.g.,
//
// type MyType struct {
//   attribute `db:"keyspace.family.column"`  // fully qualified db tag
//   attribute `db:"..column"`                // only the column identified
//   attribute `db:"column`                   // again, only the column identified
//   attribute `db:"column`
// }
func (this *storage) parse(rtype reflect.Type) *management.Error {

	// prepare database field lookups
	this.fieldToColumn = make(map[string]attribute)
	this.columnToField = make(map[string]string)

	// build lookup tables
	for i := 0; i < rtype.NumField(); i++ {

		// ///////////////////////////////////////////////////////////////////
		// pull "db" tag and parse
		f := rtype.Field(i)
		r := f.Tag.Get("db")
		if r == "" {
			continue
		}

		attrib := attribute{field: f}

		// split by comma-separated options
		commandOptions := strings.Split(r, ",")
		if len(commandOptions) > 2 {
			return management.NewError(http.StatusInternalServerError, "bad_tag", "tag on field, %v malformed, %v", f.Name, r)
		}

		// optional database type
		if len(commandOptions) > 1 {
			switch commandOptions[1] {
			case DTypeClusterKey:
			case DTypePartitionKey:
			case DTypeReadOnly:
			default:
				return management.NewError(http.StatusInternalServerError, "bad_tag", "tag on field, %v malformed, %v", f.Name, r)
			}
			attrib.dtype = commandOptions[1]
		}

		// optional keyspace, optional family and column name
		if len(commandOptions) > 0 {
			columnFamily := strings.Split(commandOptions[0], ".")
			switch len(columnFamily) {
			case 1:
				attrib.column = columnFamily[0]
			case 2:
				attrib.column = columnFamily[1]
				if columnFamily[0] != "" && this.family == "" {
					this.family = columnFamily[0]
				}
			case 3:
				attrib.column = columnFamily[2]
				if columnFamily[1] != "" && this.family == "" {
					this.family = columnFamily[1]
				}
				if columnFamily[0] != "" && this.keyspace == "" {
					this.keyspace = columnFamily[0]
				}
			default:
				return management.NewError(http.StatusInternalServerError, "bad_tag", "tag on field, %v malformed, %v", f.Name)
			}
		} else {
			// ignore empty db tag
			continue
		}

		// ///////////////////////////////////////////////////////////////////
		// pull "json" tag from the same line as "db" tag, and parse
		j := f.Tag.Get("json")
		if j == "" {
			return management.NewError(http.StatusInternalServerError, "missing_tag", "mismatched json and db tags on type")
		}
		// parse the json name
		n := strings.LastIndex(j, ",")
		k := j
		if n > 0 {
			k = j[:n]
		}
		// if json name denotes "-" (ignore), then fail
		if k == "-" {
			return management.NewError(http.StatusInternalServerError, "missing_tag", "missing required json tag name")
		}
		// if json tag doesnt have a name, use the field name
		if k == "" {
			k = f.Name
		}
		attrib.jsontag = k

		// add to lookup tables
		this.fieldToColumn[f.Name] = attrib
		this.columnToField[attrib.column] = f.Name
	}

	// postconditions
	if this.keyspace == "" || this.family == "" {
		return management.NewError(http.StatusInternalServerError, "bad_tag", "missing keyspace and/or family")
	}
	return nil
}

// Unmarshal
// Unmarshal the []byte data into a value (v) and mask (result), where the
// parameters are,
//   data    is the json data
//   target  is the target type and value reference (i.e., writable)
//
// Note that empty strings are considered unset json values, this means that
// the mask will not indicate empty strings
func (this *storage) Unmarshal(data []byte, target interface{}) (mask Mask, merr *management.Error) {
	mlog.Debug("Database.Unmarshal")

	// unmarshal actual json (typically 3875 ns/op)
	if err := json.Unmarshal(data, target); err != nil {
		return nil, management.NewError(http.StatusBadRequest, "bad_request", err.Error())
	}

	// build mask and copy struct elements (typically 1100 ns/op)
	mask = Mask{}
	tovalof := reflect.ValueOf(target)
	rtype := this.rtype
	fieldToColumn := this.fieldToColumn
	for i := 0; i < rtype.NumField(); i++ {

		// does this field use the "db" tag, and if so, determine the corresponding
		// json tag then set both the mask and the value of the unmarshaled
		// parameter. note that this loop only performs a shallow copy.
		f := rtype.Field(i)
		if a, aexists := fieldToColumn[f.Name]; aexists {

			// test empty conditions
			tofield := tovalof.Elem().Field(i)
			setmask := true
			switch tofield.Kind() {
			case reflect.Array, reflect.String:
				setmask = (tofield.Len() > 0)
			case reflect.Map, reflect.Slice:
				if setmask = !tofield.IsNil(); setmask {
					setmask = (tofield.Len() > 0)
				}
			case reflect.Struct:
				switch tofield.Interface().(type) {
				case time.Time:
					setmask = !tofield.Interface().(time.Time).Equal(time.Time{})
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				setmask = (tofield.Int() != 0)
			case reflect.Float32, reflect.Float64:
				setmask = (tofield.Float() != 0.0)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				setmask = (tofield.Uint() != 0)
			}

			// set mask column
			if setmask {
				mask[a.column] = struct{}{}
			}
		}
	}
	return mask, nil
}
