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
	"bytes"
	"errors"
	"strconv"

	"github.com/gocql/gocql"
)

const (
	INSERT Verb = "INSERT"
	UPDATE      = "UPDATE"
	SELECT      = "SELECT"
	DELETE      = "DELETE"
)

type Verb string

const (
	EQUAL        Op = "="
	LESS            = "<"
	GREATER         = ">"
	NOT             = "!"
	LESSEQUAL       = LESS + EQUAL
	GREATEREQUAL    = GREATER + EQUAL
	NOTEQUAL        = NOT + EQUAL
)

type Op string

// Builder
// Cassandra command
type Builder struct {
	keyspace, table                 string
	verb                            Verb
	limit, ttl                      int
	xaction, allowFiltering         bool
	parameters, values, wheres, ifs []Binding
}

// Binding
// Name-value binding
type Binding struct {
	name  string
	value interface{}
	ops   []Op
}

// Binding Name Accessor
func (self Binding) Name() string {
	return self.name
}

// Binding Value Accessor
func (self Binding) Value() interface{} {
	return self.value
}

// Binding Options Accessor
func (self Binding) Oper() []Op {
	return self.ops
}

// Bindings
// Name-value collection
type Bindings []Binding

// Bind
// Create a new Bindings collection
func Bind(name string, value interface{}, operators ...Op) Bindings {
	return Bindings{Binding{name: name, value: value, ops: operators}}
}

// Bind
// Add a binding to the given Bindings collection
func (self Bindings) Bind(name string, value interface{}, operators ...Op) Bindings {
	return append(self, Binding{name: name, value: value, ops: operators})
}

// NewBuilder
// Create a new database command (Insert, Update, etc)
func NewBuilder(keyspace, table string, verb Verb) *Builder {
	return new(Builder).init(keyspace, table, verb)
}

// Insert
// Create an Insert command. If you provide an IfNotExist(), the command
// will be transactional, and the Exec() bool return will
// indicate if the transaction was successful.
//
// Example,
//
//   Insert(keyspace, table).
//	   Param("accountid", accountid).Value("accountid", &accountid).
//	   Param("name", name).Value("name", &name).
//	   IfNotExist().
//	   Exec(ses)
func Insert(keyspace, table string) *Builder {
	return NewBuilder(keyspace, table, INSERT)
}

// Update
// Create an Update command. If you provide an If(), the command
// will be transactional, and the Exec() bool return will
// indicate if the transaction was successful.
//
// Example,
//
//   Update(keyspace, table).
//	   Param("accountid", accountid).Value("accountid", &accountid).
//	   Param("name", name).Value("name", &name).
//	   Where("imei", imei).
//	   If("version", version).
//	   If("accountid", "").
//	   Exec(ses)
func Update(keyspace, table string) *Builder {
	return NewBuilder(keyspace, table, UPDATE)
}

// Select
// Create an Select command.
//
// Example,
//   To perform a a single row query, use the Scan() function on
//   the command. The command *must* use Value() to get the result
//   (i.e., set the local variables to the value of each column).
//
//     command := Select( keyspace, family ).
//       Value( "column1", &column1 ).
//       Value( "column2", &column2 ).
//       Where( "column3", column3 )
//     if err := command.Scan( session ); err != nil {
//       return err
//     }
//
//  To perform a multi-select, use the Iter() and Next() functions
//  on the command. Note that the Value() references will be reset
//  after each Next().
//
//     ...
//     iter := command.Iter(ses)
//     if command.Next(iter) {
//       ... copy values to array of values ...
//     }
func Select(keyspace, table string) *Builder {
	return NewBuilder(keyspace, table, SELECT)
}

// Delete
// Create an Delete command.
//
// Example
//
//   Delete(keyspace, table).
//     Where("imei", imei).
//     Exec(ses)
func Delete(keyspace, table string) *Builder {
	return NewBuilder(keyspace, table, DELETE)
}

// init
// (Private) Common initialization function
func (this *Builder) init(keyspace, table string, verb Verb) *Builder {
	this.keyspace = keyspace
	this.table = table
	this.verb = verb
	this.limit = 0
	this.ttl = 0
	this.xaction = false
	this.allowFiltering = false
	this.parameters = make([]Binding, 0, 32)
	this.values = make([]Binding, 0)
	this.wheres = make([]Binding, 0, 2)
	this.ifs = make([]Binding, 0, 2)
	return this
}

// Param
// Add a Value (New) source variable to the Insert or Update commands.
func (this *Builder) Param(name string, value interface{}) *Builder {
	this.parameters = append(this.parameters, Binding{name, value, nil})
	return this
}

func (this *Builder) paramz() []interface{} {
	result := make([]interface{}, 0)
	for _, v := range this.parameters {
		result = append(result, v.value)
	}
	return result
}

// Value
// Add a Value (Old) target variable to the transactional Insert,
// transactional Update or Select commands.
func (this *Builder) Value(name string, value interface{}) *Builder {
	this.values = append(this.values, Binding{name, value, nil})
	return this
}

func (this *Builder) Values(values ...Binding) *Builder {
	this.values = make([]Binding, 0)
	this.values = append(this.values, values...)
	return this
}

func (this *Builder) valuez() []interface{} {
	result := make([]interface{}, 0)
	for _, v := range this.values {
		result = append(result, v.value)
	}
	return result
}

// Where
// Add a Where clause to the Update, Delete or Select commands.
func (this *Builder) Where(name string, value interface{}, operators ...Op) *Builder {
	this.wheres = append(this.wheres, Binding{name, value, operators})
	return this
}

func (this *Builder) Wheres(wheres ...Binding) *Builder {
	this.wheres = make([]Binding, 0)
	this.wheres = append(this.wheres, wheres...)
	return this
}

func (this *Builder) wherez() []interface{} {
	result := make([]interface{}, 0)
	for _, v := range this.wheres {
		result = append(result, v.value)
	}
	return result
}

// If
// Add the Update "IF" clause for transactional Updates
func (this *Builder) If(name string, value interface{}, operators ...Op) *Builder {
	this.xaction = true
	this.ifs = append(this.ifs, Binding{name, value, operators})
	return this
}

func (this *Builder) Ifs(ifs ...Binding) *Builder {
	this.ifs = make([]Binding, 0)
	this.ifs = append(this.ifs, ifs...)
	return this
}

func (this *Builder) ifz() []interface{} {
	result := make([]interface{}, 0)
	for _, v := range this.ifs {
		result = append(result, v.value)
	}
	return result
}

// IfNotExist
// Set the Insert "IF NOT EXIST" clause for transactional Inserts
func (this *Builder) IfNotExist() *Builder {
	this.xaction = true
	return this
}

// Ttl
// Set the Insert or Update time-to-live
func (this *Builder) Ttl(ttl int) *Builder {
	this.ttl = ttl
	return this
}

// Limit
// Set a Select limit (e.g., only return up to 10 or 20 rows)
func (this *Builder) Limit(limit int) *Builder {
	this.limit = limit
	return this
}

// Allow Filtering
// Set the Select "ALLOW FILTERING"
func (this *Builder) AllowFiltering() *Builder {
	this.allowFiltering = true
	return this
}

// AddToBatch
// Add this command to a batch, for example,
//
//   batch := gocql.NewBatch(gocql.LoggedBatch)
//   command1.AddToBatch(batch)
//   command2.AddToBatch(batch)
//   session.ExecuteBatch(batch)
func (this *Builder) AddToBatch(batch *gocql.Batch) {
	args := make([]interface{}, 0)
	args = append(args, this.paramz()...)
	args = append(args, this.wherez()...)
	args = append(args, this.ifz()...)

	batch.Query(this.Command(), args...)
}

// Exec
// Execute the Insert, Update or Delete command based on its parameters
func (this *Builder) Exec(ses *gocql.Session) (bool, error) {
	switch this.verb {
	case INSERT:
		return this.ExecInsert(ses)
	case UPDATE:
		return this.ExecInsert(ses)
	case DELETE:
		return this.ExecDelete(ses)
	}
	return false, nil
}

// ExecInsert
// Insert or update to the database based on the command parameters
func (this *Builder) ExecInsert(ses *gocql.Session) (bool, error) {
	result := true
	if ses == nil {
		return false, gocql.ErrNoConnections
	}
	args := make([]interface{}, 0)
	args = append(args, this.paramz()...)
	args = append(args, this.wherez()...)
	args = append(args, this.ifz()...)
	query := ses.Query(this.Command(), args...)
	if this.xaction {
		// Example,
		//
		// Insert(keyspace, table).
		//	Param("accountid", accountid).Value("accountid", &accountid).
		//	Param("name", name).Value("name", &name).
		//	IfNotExist().
		//	Exec(ses)
		//
		// Update(keyspace, table).
		//	Param("accountid", accountid).Value("accountid", &accountid).
		//	Param("name", name).Value("name", &name).
		//	Where("imei", imei).
		//	If("version", version).
		//	If("accountid", "").
		//	Exec(ses)
		var err error
		xresult := make(map[string]interface{})
		if result, err = query.MapScanCAS(xresult); err != nil {
			return false, err
		}
		for _, value := range this.values {
			value.value = xresult[value.name]
		}
	} else {
		// Example,
		//
		// Insert(keyspace, table).
		//	Param("accountid", accountid).
		//	Param("name", name).
		//	Exec(ses)
		//
		// Update(keyspace, table).
		//	Param("accountid", accountid).
		//	Param("name", name).
		//	Where("imei", imei).
		//	Exec(ses)
		if err := query.Exec(); err != nil {
			return false, err
		}
	}
	return result, nil
}

// ExecDelete
// Delete the row with the given where clause. For example,
// Delete(keyspace, table).Where("imei", imei).Exec(ses)
func (this *Builder) ExecDelete(ses *gocql.Session) (bool, error) {
	if ses == nil {
		return false, gocql.ErrNoConnections
	}

	return ses.Query(this.Command(), this.wherez()...).ScanCAS()
}

// Scan
// Select the row with the given where clause. For example,
// Select(keyspace, table).Value("accountid", &accountid).Where("imei", imei).Scan(ses)
func (this *Builder) Scan(ses *gocql.Session) error {
	if ses == nil {
		return gocql.ErrNoConnections
	}
	valuez := this.valuez()
	if len(valuez) == 0 {
		return errors.New("Builder.Scan: missing reference variables, did you forget to set them using Value()?")
	}
	return ses.Query(this.Command(), this.wherez()...).Scan(valuez...)
}

// Iter
// Create a select row iterator given the where clause
// This function is used with Next to read more than one row
func (this *Builder) Iter(ses *gocql.Session) *gocql.Iter {
	return ses.Query(this.Command(), this.wherez()...).Iter()
}

// Next
// Get the next selected row using the given iterator. For example,
// command := Select(keyspace, table).Value("name" &name).Where("accountid", accountid)
// iter := command.Iter(ses)
// more := command.Next(iter)
func (this *Builder) Next(iter *gocql.Iter) bool {
	return iter.Scan(this.valuez()...)
}

// Command
// Create the CQL command string
func (this *Builder) Command() string {
	var bb bytes.Buffer
	bb.WriteString(string(this.verb))
	bb.WriteString(" ")

	switch this.verb {
	case INSERT:
		bb.WriteString("INTO ")
		bb.WriteString(this.keyspace)
		bb.WriteString(".")
		bb.WriteString(this.table)
		bb.WriteString("(")
		bb.WriteString(this.commandParametersList())
		bb.WriteString(") VALUES (")
		bb.WriteString(this.commandOptionsList())
		bb.WriteString(")")

		if this.xaction {
			bb.WriteString(" IF NOT EXISTS")
		}
		if this.ttl > 0 {
			bb.WriteString(" USING TTL ")
			bb.WriteString(strconv.Itoa(this.ttl))
		}

	case UPDATE:
		bb.WriteString(this.keyspace)
		bb.WriteString(".")
		bb.WriteString(this.table)

		if this.ttl > 0 {
			bb.WriteString(" USING TTL ")
			bb.WriteString(strconv.Itoa(this.ttl))
		}
		bb.WriteString(" SET ")
		bb.WriteString(this.commandParameters())
		bb.WriteString(this.commandWheres())
		bb.WriteString(this.commandIfs())

	case SELECT:
		bb.WriteString(this.commandValuesList())
		bb.WriteString(" FROM ")
		bb.WriteString(this.keyspace)
		bb.WriteString(".")
		bb.WriteString(this.table)
		bb.WriteString(this.commandWheres())

		if this.limit > 0 {
			bb.WriteString(" LIMIT ")
			bb.WriteString(strconv.Itoa(this.limit))
		}
		if this.allowFiltering {
			bb.WriteString(" ALLOW FILTERING ")
		}

	case DELETE:
		bb.WriteString(this.commandParametersList())
		bb.WriteString(" FROM ")
		bb.WriteString(this.keyspace)
		bb.WriteString(".")
		bb.WriteString(this.table)
		bb.WriteString(this.commandWheres())
		bb.WriteString(" IF EXISTS")
	}
	// log.Debug("Builder.Command: %v", result)
	return bb.String()
}

func (this *Builder) commandParametersList() string {
	var bb bytes.Buffer

	for i, parameter := range this.parameters {
		if i > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(parameter.name)
	}
	return bb.String()
}

func (this *Builder) commandValuesList() string {
	var bb bytes.Buffer

	for i, value := range this.values {
		if i > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(value.name)
	}
	return bb.String()
}

func (this *Builder) commandOptionsList() string {
	var result string
	for i := range this.parameters {
		if i > 0 {
			result += ","
		}
		result += "?"
	}
	return result
}

func (this *Builder) commandParameters() string {
	var bb bytes.Buffer
	for i, parameter := range this.parameters {
		if i > 0 {
			bb.WriteString(",")
		}
		bb.WriteString(parameter.name)
		bb.WriteString("=?")
	}
	return bb.String()
}

func (this *Builder) commandWheres() string {
	var bb bytes.Buffer
	for i, where := range this.wheres {
		if i > 0 {
			bb.WriteString(" AND ")
		} else {
			bb.WriteString(" WHERE ")
		}
		op := string(EQUAL)
		if where.ops != nil && len(where.ops) > 0 {
			var opb bytes.Buffer
			for _, o := range where.ops {
				opb.WriteString(string(o))
			}
			op = opb.String()
		}
		bb.WriteString(where.name)
		bb.WriteString(op)
		bb.WriteString("?")

	}
	return bb.String()
}

func (this *Builder) commandIfs() string {
	var bb bytes.Buffer
	for i, iph := range this.ifs {
		if i > 0 {
			bb.WriteString(" AND ")
		} else {
			bb.WriteString(" IF ")
		}
		op := string(EQUAL)
		if iph.ops != nil && len(iph.ops) > 0 {
			var opb bytes.Buffer
			for _, o := range iph.ops {
				opb.WriteString(string(o))
			}
			op = opb.String()
		}
		bb.WriteString(iph.name)
		bb.WriteString(op)
		bb.WriteString("?")
	}
	return bb.String()
}

func (this *Builder) GetVerb() Verb {
	return this.verb
}

func (this *Builder) GetTableName() string {
	return this.table
}
