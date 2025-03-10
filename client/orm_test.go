package client

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
)

var (
	aString = "foo"
	aEnum   = "enum1"
	aSet    = []string{"a", "set", "of", "strings"}
	aUUID0  = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1  = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2  = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3  = "2f77b348-9768-4866-b761-89d5177ecda3"

	aUUIDSet = []string{
		aUUID0,
		aUUID1,
		aUUID2,
		aUUID3,
	}

	aIntSet = []int{
		3,
		2,
		42,
	}
	aFloat = 42.00

	aFloatSet = []float64{
		3.0,
		2.0,
		42.0,
	}

	aMap = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
)

var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "columns": {
        "aString": {
          "type": "string"
        },
        "aSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        },
        "aSingleSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "max": 1
          }
        },
        "aUUIDSet": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
          }
        },
        "aUUID": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 1,
            "max": 1
          }
        },
        "aIntSet": {
          "type": {
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aFloat": {
          "type": {
            "key": {
              "type": "real"
            }
          }
        },
        "aFloatSet": {
          "type": {
            "key": {
              "type": "real"
            },
            "min": 0,
            "max": 10
          }
        },
        "aEmptySet": {
          "type": {
            "key": {
              "type": "string"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aEnum": {
          "type": {
            "key": {
              "enum": [
                "set",
                [
                  "enum1",
                  "enum2",
                  "enum3"
                ]
              ],
              "type": "string"
            }
          }
        },
        "aMap": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)

func getOvsTestRow(t *testing.T) ovsdb.Row {
	ovsRow := ovsdb.Row{Fields: make(map[string]interface{})}
	ovsRow.Fields["aString"] = aString
	ovsRow.Fields["aSet"] = *testOvsSet(t, aSet)
	// Set's can hold the value if they have len == 1
	ovsRow.Fields["aSingleSet"] = aString

	us := make([]ovsdb.UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, ovsdb.UUID{GoUUID: u})
	}
	ovsRow.Fields["aUUIDSet"] = *testOvsSet(t, us)

	ovsRow.Fields["aUUID"] = ovsdb.UUID{GoUUID: aUUID0}

	ovsRow.Fields["aIntSet"] = *testOvsSet(t, aIntSet)

	ovsRow.Fields["aFloat"] = aFloat

	ovsRow.Fields["aFloatSet"] = *testOvsSet(t, aFloatSet)

	ovsRow.Fields["aEmptySet"] = *testOvsSet(t, []string{})

	ovsRow.Fields["aEnum"] = aEnum

	ovsRow.Fields["aMap"] = *testOvsMap(t, aMap)

	return ovsRow
}

func TestORMGetData(t *testing.T) {
	type ormTestType struct {
		AString             string            `ovs:"aString"`
		ASet                []string          `ovs:"aSet"`
		ASingleSet          []string          `ovs:"aSingleSet"`
		AUUIDSet            []string          `ovs:"aUUIDSet"`
		AUUID               string            `ovs:"aUUID"`
		AIntSet             []int             `ovs:"aIntSet"`
		AFloat              float64           `ovs:"aFloat"`
		AFloatSet           []float64         `ovs:"aFloatSet"`
		YetAnotherStringSet []string          `ovs:"aEmptySet"`
		AEnum               string            `ovs:"aEnum"`
		AMap                map[string]string `ovs:"aMap"`
		NonTagged           string
	}

	var expected = ormTestType{
		AString:             aString,
		ASet:                aSet,
		ASingleSet:          []string{aString},
		AUUIDSet:            aUUIDSet,
		AUUID:               aUUID0,
		AIntSet:             aIntSet,
		AFloat:              aFloat,
		AFloatSet:           aFloatSet,
		YetAnotherStringSet: []string{},
		AEnum:               aEnum,
		AMap:                aMap,
		NonTagged:           "something",
	}

	ovsRow := getOvsTestRow(t)
	/* Code under test */
	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	orm := newORM(&schema)
	test := ormTestType{
		NonTagged: "something",
	}
	err := orm.getRowData("TestTable", &ovsRow, &test)
	/*End code under test*/

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, expected, test)
}

func TestORMNewRow(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	tests := []struct {
		name        string
		objInput    interface{}
		expectedRow map[string]interface{}
		shoulderr   bool
	}{{
		name: "string",
		objInput: &struct {
			AString string `ovs:"aString"`
		}{
			AString: aString,
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "set",
		objInput: &struct {
			SomeSet []string `ovs:"aSet"`
		}{
			SomeSet: aSet,
		},
		expectedRow: map[string]interface{}{"aSet": testOvsSet(t, aSet)},
	}, {
		name: "emptySet with no column specification",
		objInput: &struct {
			EmptySet []string `ovs:"aSet"`
		}{
			EmptySet: []string{},
		},
		expectedRow: map[string]interface{}{},
	}, {
		name: "UUID",
		objInput: &struct {
			MyUUID string `ovs:"aUUID"`
		}{
			MyUUID: aUUID0,
		},
		expectedRow: map[string]interface{}{"aUUID": ovsdb.UUID{GoUUID: aUUID0}},
	}, {
		name: "aUUIDSet",
		objInput: &struct {
			MyUUIDSet []string `ovs:"aUUIDSet"`
		}{
			MyUUIDSet: []string{aUUID0, aUUID1},
		},
		expectedRow: map[string]interface{}{"aUUIDSet": testOvsSet(t, []ovsdb.UUID{{GoUUID: aUUID0}, {GoUUID: aUUID1}})},
	}, {
		name: "aIntSet",
		objInput: &struct {
			MyIntSet []int `ovs:"aIntSet"`
		}{
			MyIntSet: []int{0, 42},
		},
		expectedRow: map[string]interface{}{"aIntSet": testOvsSet(t, []int{0, 42})},
	}, {
		name: "aFloat",
		objInput: &struct {
			MyFloat float64 `ovs:"aFloat"`
		}{
			MyFloat: 42.42,
		},
		expectedRow: map[string]interface{}{"aFloat": 42.42},
	}, {
		name: "aFloatSet",
		objInput: &struct {
			MyFloatSet []float64 `ovs:"aFloatSet"`
		}{
			MyFloatSet: aFloatSet,
		},
		expectedRow: map[string]interface{}{"aFloatSet": testOvsSet(t, aFloatSet)},
	}, {
		name: "Enum",
		objInput: &struct {
			MyEnum string `ovs:"aEnum"`
		}{
			MyEnum: aEnum,
		},
		expectedRow: map[string]interface{}{"aEnum": aEnum},
	}, {
		name: "untagged fields should not affect row",
		objInput: &struct {
			AString string `ovs:"aString"`
			MyStuff map[string]string
		}{
			AString: aString,
			MyStuff: map[string]string{"this is": "private"},
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "Maps",
		objInput: &struct {
			MyMap map[string]string `ovs:"aMap"`
		}{
			MyMap: aMap,
		},
		expectedRow: map[string]interface{}{"aMap": testOvsMap(t, aMap)},
	},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("NewRow: %s", test.name), func(t *testing.T) {
			orm := newORM(&schema)
			row, err := orm.newRow("TestTable", test.objInput)
			if test.shoulderr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equalf(t, test.expectedRow, row, "NewRow should match expeted")
			}
		})
	}
}

func TestORMNewRowFields(t *testing.T) {
	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	type obj struct {
		MyMap    map[string]string `ovs:"aMap"`
		MySet    []string          `ovs:"aSet"`
		MyString string            `ovs:"aString"`
		MyFloat  float64           `ovs:"aFloat"`
	}
	testObj := obj{}

	tests := []struct {
		name        string
		prepare     func(*obj)
		expectedRow map[string]interface{}
		fields      []interface{}
		err         bool
	}{{
		name: "string",
		prepare: func(o *obj) {
			o.MyString = aString
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "empty string with field specification",
		prepare: func(o *obj) {
			o.MyString = ""
		},
		fields:      []interface{}{&testObj.MyString},
		expectedRow: map[string]interface{}{"aString": ""},
	}, {
		name: "empty set without field specification",
		prepare: func(o *obj) {
		},
		expectedRow: map[string]interface{}{},
	}, {
		name: "empty set without field specification",
		prepare: func(o *obj) {
		},
		fields:      []interface{}{&testObj.MySet},
		expectedRow: map[string]interface{}{"aSet": testOvsSet(t, []string{})},
	}, {
		name: "empty maps",
		prepare: func(o *obj) {
			o.MyString = "foo"
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "empty maps with field specification",
		prepare: func(o *obj) {
			o.MyString = "foo"
		},
		fields:      []interface{}{&testObj.MyMap},
		expectedRow: map[string]interface{}{"aMap": testOvsMap(t, map[string]string{})},
	}, {
		name: "Complex object with field selection",
		prepare: func(o *obj) {
			o.MyString = aString
			o.MyMap = aMap
			o.MySet = aSet
			o.MyFloat = aFloat
		},
		fields:      []interface{}{&testObj.MyMap, &testObj.MySet},
		expectedRow: map[string]interface{}{"aMap": testOvsMap(t, aMap), "aSet": testOvsSet(t, aSet)},
	},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("NewRow: %s", test.name), func(t *testing.T) {
			orm := newORM(&schema)
			// Clean the test object
			testObj.MyString = ""
			testObj.MyMap = nil
			testObj.MySet = nil
			testObj.MyFloat = 0

			test.prepare(&testObj)
			row, err := orm.newRow("TestTable", &testObj, test.fields...)
			if test.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equalf(t, test.expectedRow, row, "NewRow should match expeted")
			}
		})
	}
}

func TestORMCondition(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "indexes": [["name"],["composed_1","composed_2"]],
      "columns": {
        "name": {
          "type": "string"
        },
        "composed_1": {
          "type": {
            "key": "string"
          }
        },
        "composed_2": {
          "type": {
            "key": "string"
          }
        },
        "config": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)
	type testType struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
	}

	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	orm := newORM(&schema)

	type Test struct {
		name     string
		prepare  func(*testType)
		expected []ovsdb.Condition
		index    []interface{}
		err      bool
	}
	testObj := testType{}

	tests := []Test{
		{
			name: "simple index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{},
			expected: []ovsdb.Condition{{Column: "name", Function: ovsdb.ConditionEqual, Value: "foo"}},
			err:      false,
		},
		{
			name: "UUID",
			prepare: func(t *testType) {
				t.ID = aUUID0
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{},
			expected: []ovsdb.Condition{{Column: "_uuid", Function: ovsdb.ConditionEqual, Value: ovsdb.UUID{GoUUID: aUUID0}}},
			err:      false,
		},
		{
			name: "specify index",
			prepare: func(t *testType) {
				t.ID = aUUID0
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{&testObj.MyName},
			expected: []ovsdb.Condition{{Column: "name", Function: ovsdb.ConditionEqual, Value: "foo"}},
			err:      false,
		},
		{
			name: "complex index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = ""
				t.Config = nil
				t.Comp1 = "foo"
				t.Comp2 = "bar"
			},
			expected: []ovsdb.Condition{
				{Column: "composed_1", Function: ovsdb.ConditionEqual, Value: "foo"},
				{Column: "composed_2", Function: ovsdb.ConditionEqual, Value: "bar"}},
			index: []interface{}{},
			err:   false,
		},
		{
			name: "first index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = "something"
				t.Config = nil
				t.Comp1 = "foo"
				t.Comp2 = "bar"
			},
			expected: []ovsdb.Condition{{Column: "name", Function: ovsdb.ConditionEqual, Value: "something"}},
			index:    []interface{}{},
			err:      false,
		},
		{
			name: "Error: None",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = ""
				t.Config = map[string]string{"foo": "bar"}
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index: []interface{}{},
			err:   true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("newEqualityCondition_%s", tt.name), func(t *testing.T) {
			tt.prepare(&testObj)
			conds, err := orm.newEqualityCondition("TestTable", &testObj, tt.index...)
			if tt.err {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
			} else {
				if err != nil {
					t.Error(err)
				}
				if !assert.ElementsMatch(t, tt.expected, conds, "Condition must match expected") {
					t.Logf("%v \n", conds)
				}
			}

		})
	}
}

func TestORMEqualIndexes(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "indexes": [["name"],["composed_1","composed_2"]],
      "columns": {
        "name": {
          "type": "string"
        },
        "composed_1": {
          "type": {
            "key": "string"
          }
        },
        "composed_2": {
          "type": {
            "key": "string"
          }
        },
        "int1": {
          "type": {
            "key": "integer"
          }
        },
        "int2": {
          "type": {
            "key": "integer"
          }
        },
        "config": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)
	type testType struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
		Int1   int               `ovs:"int1"`
		Int2   int               `ovs:"int2"`
	}

	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	orm := newORM(&schema)

	type Test struct {
		name     string
		obj1     testType
		obj2     testType
		expected bool
		indexes  []string
	}
	tests := []Test{
		{
			name: "same simple index",
			obj1: testType{
				MyName: "foo",
			},
			obj2: testType{
				MyName: "foo",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "diff simple index",
			obj1: testType{
				MyName: "foo",
			},
			obj2: testType{
				MyName: "bar",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same uuid",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			obj2: testType{
				ID:     aUUID0,
				MyName: "bar",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "diff uuid",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "bar",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same complex_index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "bar",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "different",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same additional index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
			},
			expected: true,
			indexes:  []string{"int1"},
		},
		{
			name: "diff additional index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   420,
			},
			expected: false,
			indexes:  []string{"int1"},
		},
		{
			name: "same additional indexes ",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
				Int2:   25,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
				Int2:   25,
			},
			expected: true,
			indexes:  []string{"int1", "int2"},
		},
		{
			name: "diff additional indexes ",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
				Int2:   50,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
				Int2:   25,
			},
			expected: false,
			indexes:  []string{"int1", "int2"},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("Equal %s", test.name), func(t *testing.T) {
			eq, err := orm.equalIndexes(orm.schema.Table("TestTable"), &test.obj1, &test.obj2, test.indexes...)
			assert.Nil(t, err)
			assert.Equalf(t, test.expected, eq, "equal value should match expected")
		})
	}

	// Test we can also use field pointers
	obj1 := testType{
		ID:     aUUID0,
		MyName: "name1",
		Comp1:  "foo",
		Comp2:  "bar1",
		Int1:   42,
		Int2:   25,
	}
	obj2 := testType{
		ID:     aUUID1,
		MyName: "name2",
		Comp1:  "foo",
		Comp2:  "bar2",
		Int1:   42,
		Int2:   25,
	}
	eq, err := orm.equalFields("TestTable", &obj1, &obj2, &obj1.Int1, &obj1.Int2)
	assert.Nil(t, err)
	assert.True(t, eq)
	// Useing pointers to second value is not supported
	_, err = orm.equalFields("TestTable", &obj1, &obj2, &obj2.Int1, &obj2.Int2)
	assert.NotNil(t, err)

}

func TestORMMutation(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "columns": {
        "string": {
          "type": "string"
        },
        "set": {
          "type": {
            "key": "string",
            "min": 0
          }
        },
        "map": {
          "type": {
            "key": "string",
            "value": "string"
          }
        },
        "unmutable": {
	  "mutable": false,
          "type": {
            "key": "integer"
          }
	},
        "int": {
          "type": {
            "key": "integer"
          }
	}
      }
    }
  }
}`)
	type testType struct {
		UUID      string            `ovs:"_uuid"`
		String    string            `ovs:"string"`
		Set       []string          `ovs:"set"`
		Map       map[string]string `ovs:"map"`
		Int       int               `ovs:"int"`
		UnMutable int               `ovs:"unmutable"`
	}

	var schema ovsdb.DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	orm := newORM(&schema)

	type Test struct {
		name     string
		column   string
		obj      testType
		expected []interface{}
		mutator  ovsdb.Mutator
		value    interface{}
		err      bool
	}
	tests := []Test{
		{
			name:    "string",
			column:  "string",
			obj:     testType{},
			mutator: ovsdb.MutateOperationAdd,
			err:     true,
		},
		{
			name:     "Increment integer",
			column:   "int",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationAdd,
			value:    1,
			expected: []interface{}{"int", ovsdb.MutateOperationAdd, 1},
			err:      false,
		},
		{
			name:     "Increment integer",
			column:   "int",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationModulo,
			value:    2,
			expected: []interface{}{"int", ovsdb.MutateOperationModulo, 2},
			err:      false,
		},
		{
			name:    "non-mutable",
			column:  "unmutable",
			obj:     testType{},
			mutator: ovsdb.MutateOperationSubstract,
			value:   2,
			err:     true,
		},
		{
			name:     "Add elemet to set ",
			column:   "set",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationInsert,
			value:    []string{"foo"},
			expected: []interface{}{"set", ovsdb.MutateOperationInsert, testOvsSet(t, []string{"foo"})},
			err:      false,
		},
		{
			name:     "Delete element from set ",
			column:   "set",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationDelete,
			value:    []string{"foo"},
			expected: []interface{}{"set", ovsdb.MutateOperationDelete, testOvsSet(t, []string{"foo"})},
			err:      false,
		},
		{
			name:     "Delete elements from map ",
			column:   "map",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationDelete,
			value:    []string{"foo", "bar"},
			expected: []interface{}{"map", ovsdb.MutateOperationDelete, testOvsSet(t, []string{"foo", "bar"})},
			err:      false,
		},
		{
			name:     "Insert elements in map ",
			column:   "map",
			obj:      testType{},
			mutator:  ovsdb.MutateOperationInsert,
			value:    map[string]string{"foo": "bar"},
			expected: []interface{}{"map", ovsdb.MutateOperationInsert, testOvsMap(t, map[string]string{"foo": "bar"})},
			err:      false,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("newMutation%s", test.name), func(t *testing.T) {
			mutation, err := orm.newMutation("TestTable", &test.obj, test.column, test.mutator, test.value)
			if test.err {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
			} else {
				if err != nil {
					t.Error(err)
				}
			}

			assert.Equalf(t, test.expected, mutation, "Mutation must match expected")
		})
	}
}

func testOvsSet(t *testing.T, set interface{}) *ovsdb.OvsSet {
	oSet, err := ovsdb.NewOvsSet(set)
	assert.Nil(t, err)
	return oSet
}

func testOvsMap(t *testing.T, set interface{}) *ovsdb.OvsMap {
	oMap, err := ovsdb.NewOvsMap(set)
	assert.Nil(t, err)
	return oMap
}
