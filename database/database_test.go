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
	"encoding/json"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
	"time"
)

type Full struct {
	Name             string              `json:"n" db:"test.mytype.name"`
	Value            int                 `json:"v" db:"value"`
	Value64          int64               `json:"v64" db:"v64"`
	Bool             bool                `json:"b" db:"b"`
	Byte             byte                `json:"by" db:"by"`
	StringArray      []string            `json:"sa" db:"sa"`
	ByteArray        []byte              `json:"ba" db:"ba"`
	Tock             time.Time           `json:"t" db:"t"`
	MapString2String map[string]string   `json:"ms2s" db:"ms2s"`
	MapString2Struct map[string]struct{} `json:"ms2t" db:"ms2t"`
}

func BenchmarkUnmarshal(b *testing.B) {
	original := Full{
		Name:        "Joe",
		Value:       4,
		Value64:     5,
		Bool:        true,
		Byte:        byte(15),
		StringArray: []string{"one", "two"},
		ByteArray:   []byte{12, 45, 15},
		Tock:        time.Now(),
	}
	storage, err := NewMemory(reflect.TypeOf(Full{}))
	data, _ := json.Marshal(original)

	var target Full
	var mask Mask
	for n := 0; n < b.N; n++ {
		mask, err = storage.Unmarshal(data, &target)

	}
	_ = mask
	_ = err
}

func BenchmarkGolangUnmarshal(b *testing.B) {
	original := Full{
		Name:        "Joe",
		Value:       4,
		Value64:     5,
		Bool:        true,
		Byte:        byte(15),
		StringArray: []string{"one", "two"},
		ByteArray:   []byte{12, 45, 15},
		Tock:        time.Now(),
	}
	data, _ := json.Marshal(original)

	var target Full
	var mask Mask
	var goerr error
	for n := 0; n < b.N; n++ {
		goerr = json.Unmarshal(data, &target)
	}
	_ = mask
	_ = goerr
}

// test unmarshal functions
func TestUnmarshal(t *testing.T) {

	convey.Convey("Test Unmarshal", t, func() {

		convey.Convey("Test Simple Unmarshal", func() {
			//mlog.EnableDebug(true)

			storage, err := NewMemory(reflect.TypeOf(Full{}))
			convey.So(err, convey.ShouldBeNil)
			original := Full{
				//Name:        "Joe",
				Value:   4,
				Value64: 5,
				Bool:    true,
				//Byte:    byte(15),
				//StringArray: []string{"one", "two"},
				ByteArray: []byte{12, 45, 15},
				Tock:      time.Now(),
			}
			data, _ := json.Marshal(original)

			var target Full
			var mask Mask
			mask, err = storage.Unmarshal(data, &target)
			convey.So(err, convey.ShouldBeNil)
			convey.So(mask["name"], convey.ShouldNotBeNil)
			t.Logf("mask, %v\n", mask)
			t.Logf("target, %v\n", target)
		})
	})
}

func TestBinding(t *testing.T) {
	convey.Convey("Test Binding", t, func() {
		bindings := Bindings{}
		a := 1
		b := 2
		bindings = bindings.Bind("hello", a)
		bindings = bindings.Bind("goodbye", b)
		convey.So(bindings[0].name, convey.ShouldEqual, "hello")
		convey.So(bindings[1].name, convey.ShouldEqual, "goodbye")
	})
}

func TestSelect(t *testing.T) {
	convey.Convey("Test Select", t, func() {
		convey.Convey("Test Simple Select", func() {
			var param1, param2 int
			command := Select("my_keyspace", "my_table").
				Value("param1", param1).
				Value("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "SELECT param1,param2 FROM my_keyspace.my_table")
		})
		convey.Convey("Test Select w/Where clause", func() {
			var param1, param2 int
			command := Select("my_keyspace", "my_table").
				Value("param1", param1).
				Value("param2", param2).
				Where("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "SELECT param1,param2 FROM my_keyspace.my_table WHERE param2=?")
		})
		convey.Convey("Test Select w/Where and Limit clauses", func() {
			var param1, param2 int
			command := Select("my_keyspace", "my_table").
				Value("param1", param1).
				Value("param2", param2).
				Where("param2", param2).
				Limit(5).
				Command()
			convey.So(command, convey.ShouldEqual, "SELECT param1,param2 FROM my_keyspace.my_table WHERE param2=? LIMIT 5")
		})
		convey.Convey("Test Select w/Where using different where operators", func() {
			var param1, param2 int
			command := Select("my_keyspace", "my_table").
				Value("param1", param1).
				Value("param2", param2).
				Where("param2", param2, GREATEREQUAL).
				Where("param1", param1, LESSEQUAL).
				Command()
			convey.So(command, convey.ShouldEqual, "SELECT param1,param2 FROM my_keyspace.my_table WHERE param2>=? AND param1<=?")
		})
	})
}

func TestUpdate(t *testing.T) {
	convey.Convey("Test Update", t, func() {
		convey.Convey("Test Non-transactional Update w/Where clause", func() {
			var param1, param2 int
			command := Update("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				Where("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "UPDATE my_keyspace.my_table SET param1=?,param2=? WHERE param2=?")
		})
		convey.Convey("Test Transactional Update w/Where clause", func() {
			var param1, param2, version int
			command := Update("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				Where("param2", param2).
				If("version", version).
				Command()
			convey.So(command, convey.ShouldEqual, "UPDATE my_keyspace.my_table SET param1=?,param2=? WHERE param2=? IF version=?")
		})
	})
}

func TestInsert(t *testing.T) {
	convey.Convey("Test Insert", t, func() {
		convey.Convey("Test Non-Transactional Insert", func() {
			var param1, param2 int
			command := Insert("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "INSERT INTO my_keyspace.my_table(param1,param2) VALUES (?,?)")
		})
		convey.Convey("Test Transactional Insert", func() {
			var param1, param2 int
			command := Insert("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				IfNotExist().
				Command()
			convey.So(command, convey.ShouldEqual, "INSERT INTO my_keyspace.my_table(param1,param2) VALUES (?,?) IF NOT EXISTS")
		})
	})
}

func TestDelete(t *testing.T) {
	convey.Convey("Test Delete", t, func() {
		convey.Convey("Test Simple Delete", func() {
			var param1, param2 int
			command := Delete("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "DELETE param1,param2 FROM my_keyspace.my_table")
		})
		convey.Convey("Test Delete w/Where clause", func() {
			var param1, param2 int
			command := Delete("my_keyspace", "my_table").
				Param("param1", param1).
				Param("param2", param2).
				Where("param2", param2).
				Command()
			convey.So(command, convey.ShouldEqual, "DELETE param1,param2 FROM my_keyspace.my_table WHERE param2=?")
		})
	})
}

// create keyspace if not exists test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// create table if not exists test.mytype(
//   name text,
//   value bigint,
//   column text,
//   primary key(name,value)
// );
// create index if not exists mytype_idx on test.mytype(value);
type MyType struct {
	Name   string `json:"n" db:"test.mytype.name,partitionkey"`
	Value  int    `json:"v" db:"value,clusterkey"`
	Column string `json:"c" db:"column"`
}

func TestStorage(t *testing.T) {

	database, err := NewMemory(reflect.TypeOf(MyType{}))
	if err != nil {
		return
	}

	convey.Convey("Test Insert and Select", t, func() {

		convey.Convey("Test Insert", func() {
			mytype1 := MyType{
				Name:   "bill",
				Value:  5,
				Column: "hello",
			}
			mytype2 := MyType{
				Name:   "fred",
				Value:  6,
				Column: "hello",
			}
			mytype3 := MyType{
				Name:   "greg",
				Value:  7,
				Column: "goodbye",
			}
			data := []interface{}{mytype1, mytype2, mytype3}
			err := database.InsertMany(data...)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("Test Single Select", func() {
			result, err := database.Query(Bind("name", "bill")...)
			convey.So(err, convey.ShouldBeNil)
			convey.So(result, convey.ShouldNotBeNil)
			mytype := result.(MyType)
			convey.So(mytype.Value, convey.ShouldEqual, 5)

			result, err = database.Query(Bind("name", "fred")...)
			convey.So(err, convey.ShouldBeNil)
			convey.So(result, convey.ShouldNotBeNil)
			mytype = result.(MyType)
			convey.So(mytype.Value, convey.ShouldEqual, 6)
		})

		convey.Convey("Test Multi Select", func() {
			results, err := database.QueryMany(0, Bind("column", "hello")...)
			convey.So(err, convey.ShouldBeNil)
			convey.So(results, convey.ShouldNotBeNil)
			convey.So(len(results), convey.ShouldEqual, 2)

			wheres := Bind("column", "hello")
			wheres = wheres.Bind("value", 6)
			results, err = database.QueryMany(0, wheres...)
			convey.So(err, convey.ShouldBeNil)
			convey.So(results, convey.ShouldNotBeNil)
			convey.So(len(results), convey.ShouldEqual, 1)
		})

		convey.Convey("Test Delete", func() {
			err := database.Delete(Bind("column", "hello")...)
			convey.So(err, convey.ShouldBeNil)

			_, err = database.QueryMany(0, Bind("column", "hello")...)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
