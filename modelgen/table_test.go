package modelgen

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"text/template"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/example/vswitchd"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTableTemplate(t *testing.T) {
	rawSchema := []byte(`
	{
		"name": "AtomicDB",
		"version": "0.0.0",
		"tables": {
			"atomicTable": {
				"columns": {
					"str": {
						"type": "string"
					},
					"int": {
						"type": "integer"
					},
					"float": {
						"type": "real"
					},
					"protocol": {
						"type": {"key": {"type": "string",
								 "enum": ["set", ["tcp", "udp", "sctp"]]},
								 "min": 0, "max": 1}},
					"event_type": {"type": {"key": {"type": "string",
													"enum": ["set", ["empty_lb_backends"]]}}}
				}
			}
		}
	}`)

	test := []struct {
		name      string
		extend    func(tmpl *template.Template, data TableTemplateData)
		expected  string
		err       bool
		formatErr bool
	}{
		{
			name: "normal",
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

const AtomicTableTable = "atomicTable"

type (
	AtomicTableEventType = string
	AtomicTableProtocol  = string
)

var (
	AtomicTableEventTypeEmptyLbBackends AtomicTableEventType = "empty_lb_backends"
	AtomicTableProtocolTCP              AtomicTableProtocol  = "tcp"
	AtomicTableProtocolUDP              AtomicTableProtocol  = "udp"
	AtomicTableProtocolSCTP             AtomicTableProtocol  = "sctp"
)

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string               ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType AtomicTableEventType ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64              ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int                  ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *AtomicTableProtocol ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string               ` + "`" + `ovsdb:"str"` + "`" + `
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}
`,
		},
		{
			name: "no enums",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				data.WithEnumTypes(false)
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

const AtomicTableTable = "atomicTable"

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string  ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType string  ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64 ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int     ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *string ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string  ` + "`" + `ovsdb:"str"` + "`" + `
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}
`,
		},
		{
			name: "add fields using same data",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				extra := `{{ define "extraFields" }} {{- $tableName := index . "TableName" }} {{ range $field := index . "Fields"  }}	Other{{ FieldName $field.Column }}  {{ FieldType $tableName $field.Column $field.Schema }}
{{ end }}
{{- end }}`
				_, err := tmpl.Parse(extra)
				if err != nil {
					panic(err)
				}
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

const AtomicTableTable = "atomicTable"

type (
	AtomicTableEventType = string
	AtomicTableProtocol  = string
)

var (
	AtomicTableEventTypeEmptyLbBackends AtomicTableEventType = "empty_lb_backends"
	AtomicTableProtocolTCP              AtomicTableProtocol  = "tcp"
	AtomicTableProtocolUDP              AtomicTableProtocol  = "udp"
	AtomicTableProtocolSCTP             AtomicTableProtocol  = "sctp"
)

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string               ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType AtomicTableEventType ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64              ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int                  ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *AtomicTableProtocol ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string               ` + "`" + `ovsdb:"str"` + "`" + `

	OtherUUID      string
	OtherEventType string
	OtherFloat     float64
	OtherInt       int
	OtherProtocol  *string
	OtherStr       string
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}
`,
		},
		{
			name: "with deep copy code",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				data.WithExtendedGen(true)
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

import "github.com/ovn-org/libovsdb/model"

const AtomicTableTable = "atomicTable"

type (
	AtomicTableEventType = string
	AtomicTableProtocol  = string
)

var (
	AtomicTableEventTypeEmptyLbBackends AtomicTableEventType = "empty_lb_backends"
	AtomicTableProtocolTCP              AtomicTableProtocol  = "tcp"
	AtomicTableProtocolUDP              AtomicTableProtocol  = "udp"
	AtomicTableProtocolSCTP             AtomicTableProtocol  = "sctp"
)

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string               ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType AtomicTableEventType ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64              ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int                  ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *AtomicTableProtocol ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string               ` + "`" + `ovsdb:"str"` + "`" + `
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}

func (a *AtomicTable) GetUUID() string {
	return a.UUID
}

func (a *AtomicTable) GetEventType() AtomicTableEventType {
	return a.EventType
}

func (a *AtomicTable) GetFloat() float64 {
	return a.Float
}

func (a *AtomicTable) GetInt() int {
	return a.Int
}

func (a *AtomicTable) GetProtocol() *AtomicTableProtocol {
	return a.Protocol
}

func copyAtomicTableProtocol(a *AtomicTableProtocol) *AtomicTableProtocol {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalAtomicTableProtocol(a, b *AtomicTableProtocol) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *AtomicTable) GetStr() string {
	return a.Str
}

func (a *AtomicTable) DeepCopyInto(b *AtomicTable) {
	*b = *a
	b.Protocol = copyAtomicTableProtocol(a.Protocol)
}

func (a *AtomicTable) DeepCopy() *AtomicTable {
	b := new(AtomicTable)
	a.DeepCopyInto(b)
	return b
}

func (a *AtomicTable) CloneModelInto(b model.Model) {
	c := b.(*AtomicTable)
	a.DeepCopyInto(c)
}

func (a *AtomicTable) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *AtomicTable) Equals(b *AtomicTable) bool {
	return a.UUID == b.UUID &&
		a.EventType == b.EventType &&
		a.Float == b.Float &&
		a.Int == b.Int &&
		equalAtomicTableProtocol(a.Protocol, b.Protocol) &&
		a.Str == b.Str
}

func (a *AtomicTable) EqualsModel(b model.Model) bool {
	c := b.(*AtomicTable)
	return a.Equals(c)
}

var _ model.CloneableModel = &AtomicTable{}
var _ model.ComparableModel = &AtomicTable{}
`,
		},
		{
			name: "with deep copy code and extra fields",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				data.WithExtendedGen(true)
				extra := `{{ define "extraFields" }} {{- $tableName := index . "TableName" }} {{ range $field := index . "Fields"  }}	Other{{ FieldName $field.Column }}  {{ FieldType $tableName $field.Column $field.Schema }}
{{ end }}
{{- end }}
{{ define "extraImports" }}
import "fmt"
{{ end }}
{{ define "extraDefinitions" }}
func copyAtomicTableOtherProtocol(a *AtomicTableProtocol) *AtomicTableProtocol {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalAtomicTableOtherProtocol(a, b *AtomicTableProtocol) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *AtomicTable) PrintAtomicTableOtherProtocol() bool {
	fmt.Printf(a.OtherProtocol)
}
{{ end }}
{{ define "deepCopyExtraFields" }}
	b.OtherProtocol = copyAtomicTableOtherProtocol(a.OtherProtocol)
{{- end }}
{{ define "equalExtraFields" }} &&
	equalAtomicTableOtherProtocol(a.OtherProtocol, b.OtherProtocol)
{{- end }}
`
				_, err := tmpl.Parse(extra)
				if err != nil {
					panic(err)
				}
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

import "github.com/ovn-org/libovsdb/model"

import "fmt"

const AtomicTableTable = "atomicTable"

type (
	AtomicTableEventType = string
	AtomicTableProtocol  = string
)

var (
	AtomicTableEventTypeEmptyLbBackends AtomicTableEventType = "empty_lb_backends"
	AtomicTableProtocolTCP              AtomicTableProtocol  = "tcp"
	AtomicTableProtocolUDP              AtomicTableProtocol  = "udp"
	AtomicTableProtocolSCTP             AtomicTableProtocol  = "sctp"
)

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string               ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType AtomicTableEventType ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64              ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int                  ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *AtomicTableProtocol ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string               ` + "`" + `ovsdb:"str"` + "`" + `

	OtherUUID      string
	OtherEventType string
	OtherFloat     float64
	OtherInt       int
	OtherProtocol  *string
	OtherStr       string
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}

func copyAtomicTableOtherProtocol(a *AtomicTableProtocol) *AtomicTableProtocol {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalAtomicTableOtherProtocol(a, b *AtomicTableProtocol) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *AtomicTable) PrintAtomicTableOtherProtocol() bool {
	fmt.Printf(a.OtherProtocol)
}

func (a *AtomicTable) GetUUID() string {
	return a.UUID
}

func (a *AtomicTable) GetEventType() AtomicTableEventType {
	return a.EventType
}

func (a *AtomicTable) GetFloat() float64 {
	return a.Float
}

func (a *AtomicTable) GetInt() int {
	return a.Int
}

func (a *AtomicTable) GetProtocol() *AtomicTableProtocol {
	return a.Protocol
}

func copyAtomicTableProtocol(a *AtomicTableProtocol) *AtomicTableProtocol {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalAtomicTableProtocol(a, b *AtomicTableProtocol) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *AtomicTable) GetStr() string {
	return a.Str
}

func (a *AtomicTable) DeepCopyInto(b *AtomicTable) {
	*b = *a
	b.Protocol = copyAtomicTableProtocol(a.Protocol)
	b.OtherProtocol = copyAtomicTableOtherProtocol(a.OtherProtocol)
}

func (a *AtomicTable) DeepCopy() *AtomicTable {
	b := new(AtomicTable)
	a.DeepCopyInto(b)
	return b
}

func (a *AtomicTable) CloneModelInto(b model.Model) {
	c := b.(*AtomicTable)
	a.DeepCopyInto(c)
}

func (a *AtomicTable) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *AtomicTable) Equals(b *AtomicTable) bool {
	return a.UUID == b.UUID &&
		a.EventType == b.EventType &&
		a.Float == b.Float &&
		a.Int == b.Int &&
		equalAtomicTableProtocol(a.Protocol, b.Protocol) &&
		a.Str == b.Str &&
		equalAtomicTableOtherProtocol(a.OtherProtocol, b.OtherProtocol)
}

func (a *AtomicTable) EqualsModel(b model.Model) bool {
	c := b.(*AtomicTable)
	return a.Equals(c)
}

var _ model.CloneableModel = &AtomicTable{}
var _ model.ComparableModel = &AtomicTable{}
`,
		},
		{
			name: "with deep copy code but no enums",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				data.WithExtendedGen(true)
				data.WithEnumTypes(false)
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

import "github.com/ovn-org/libovsdb/model"

const AtomicTableTable = "atomicTable"

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string  ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType string  ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64 ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int     ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *string ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string  ` + "`" + `ovsdb:"str"` + "`" + `
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}

func (a *AtomicTable) GetUUID() string {
	return a.UUID
}

func (a *AtomicTable) GetEventType() string {
	return a.EventType
}

func (a *AtomicTable) GetFloat() float64 {
	return a.Float
}

func (a *AtomicTable) GetInt() int {
	return a.Int
}

func (a *AtomicTable) GetProtocol() *string {
	return a.Protocol
}

func copyAtomicTableProtocol(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalAtomicTableProtocol(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *AtomicTable) GetStr() string {
	return a.Str
}

func (a *AtomicTable) DeepCopyInto(b *AtomicTable) {
	*b = *a
	b.Protocol = copyAtomicTableProtocol(a.Protocol)
}

func (a *AtomicTable) DeepCopy() *AtomicTable {
	b := new(AtomicTable)
	a.DeepCopyInto(b)
	return b
}

func (a *AtomicTable) CloneModelInto(b model.Model) {
	c := b.(*AtomicTable)
	a.DeepCopyInto(c)
}

func (a *AtomicTable) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *AtomicTable) Equals(b *AtomicTable) bool {
	return a.UUID == b.UUID &&
		a.EventType == b.EventType &&
		a.Float == b.Float &&
		a.Int == b.Int &&
		equalAtomicTableProtocol(a.Protocol, b.Protocol) &&
		a.Str == b.Str
}

func (a *AtomicTable) EqualsModel(b model.Model) bool {
	c := b.(*AtomicTable)
	return a.Equals(c)
}

var _ model.CloneableModel = &AtomicTable{}
var _ model.ComparableModel = &AtomicTable{}
`,
		},
		{
			name: "add extra functions using extra data",
			extend: func(tmpl *template.Template, data TableTemplateData) {
				extra := `{{ define "postStructDefinitions" }}
func {{ index . "TestName" }} () string {
	return "{{ index . "StructName" }}"
} {{ end }}
`
				_, err := tmpl.Parse(extra)
				if err != nil {
					panic(err)
				}
				data["TestName"] = "TestFunc"
			},
			expected: `// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package test

const AtomicTableTable = "atomicTable"

type (
	AtomicTableEventType = string
	AtomicTableProtocol  = string
)

var (
	AtomicTableEventTypeEmptyLbBackends AtomicTableEventType = "empty_lb_backends"
	AtomicTableProtocolTCP              AtomicTableProtocol  = "tcp"
	AtomicTableProtocolUDP              AtomicTableProtocol  = "udp"
	AtomicTableProtocolSCTP             AtomicTableProtocol  = "sctp"
)

// AtomicTable defines an object in atomicTable table
type AtomicTable struct {
	UUID      string               ` + "`" + `ovsdb:"_uuid"` + "`" + `
	EventType AtomicTableEventType ` + "`" + `ovsdb:"event_type"` + "`" + `
	Float     float64              ` + "`" + `ovsdb:"float"` + "`" + `
	Int       int                  ` + "`" + `ovsdb:"int"` + "`" + `
	Protocol  *AtomicTableProtocol ` + "`" + `ovsdb:"protocol"` + "`" + `
	Str       string               ` + "`" + `ovsdb:"str"` + "`" + `
}

func (a *AtomicTable) Table() string {
	return AtomicTableTable
}

func TestFunc() string {
	return "AtomicTable"
}
`,
		},
		{
			name:      "add bad code",
			formatErr: true,
			extend: func(tmpl *template.Template, data TableTemplateData) {
				extra := `{{ define "preStructDefinitions" }}
WRONG FORMAT
{{ end }}
`
				_, err := tmpl.Parse(extra)
				if err != nil {
					panic(err)
				}
			},
		},
	}

	var schema ovsdb.DatabaseSchema
	err := json.Unmarshal(rawSchema, &schema)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range test {
		t.Run(fmt.Sprintf("Table Test: %s", tt.name), func(t *testing.T) {
			fakeTable := "atomicTable"
			tmpl := NewTableTemplate()
			table := schema.Tables[fakeTable]
			data := GetTableTemplateData(
				"test",
				fakeTable,
				&table,
			)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				if tt.extend != nil {
					tt.extend(tmpl, data)
				}
				for i := 0; i < 3; i++ {
					g, err := NewGenerator()
					require.NoError(t, err)
					b, err := g.Format(tmpl, data)
					if tt.formatErr {
						assert.NotNil(t, err)
					} else {
						require.NoError(t, err)
						assert.Equal(t, tt.expected, string(b))
					}
				}
			}
		})
	}
}

func TestFieldName(t *testing.T) {
	cases := []struct {
		in       string
		expected string
	}{
		{"foo", "Foo"},
	}
	for _, tt := range cases {
		if s := FieldName(tt.in); s != tt.expected {
			t.Fatalf("got %s, wanted %s", s, tt.expected)
		}
	}

}

func TestStructName(t *testing.T) {
	if s := StructName("Foo_Bar"); s != "FooBar" {
		t.Fatalf("got %s, wanted FooBar", s)
	}
}

func TestFieldType(t *testing.T) {
	singleValueSet := `{
		"type": {
			"key": {
				"type": "string"
			},
			"min": 0
		}
	}`
	singleValueSetSchema := ovsdb.ColumnSchema{}
	err := json.Unmarshal([]byte(singleValueSet), &singleValueSetSchema)
	require.NoError(t, err)

	multipleValueSet := `{
		"type": {
			"key": {
				"type": "string"
			},
			"min": 0,
			"max": 2
		}
	}`
	multipleValueSetSchema := ovsdb.ColumnSchema{}
	err = json.Unmarshal([]byte(multipleValueSet), &multipleValueSetSchema)
	require.NoError(t, err)

	tests := []struct {
		tableName  string
		columnName string
		in         *ovsdb.ColumnSchema
		out        string
	}{
		{"t1", "c1", &singleValueSetSchema, "*string"},
		{"t1", "c2", &multipleValueSetSchema, "[]string"},
	}

	for _, tt := range tests {
		if got := FieldType(tt.tableName, tt.columnName, tt.in); got != tt.out {
			t.Errorf("FieldType() = %v, want %v", got, tt.out)
		}
	}
}

func TestAtomicType(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
	}{
		{"IntegerToInt", ovsdb.TypeInteger, "int"},
		{"RealToFloat", ovsdb.TypeReal, "float64"},
		{"BooleanToBool", ovsdb.TypeBoolean, "bool"},
		{"StringToString", ovsdb.TypeString, "string"},
		{"UUIDToString", ovsdb.TypeUUID, "string"},
		{"Invalid", "notAType", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AtomicType(tt.in); got != tt.out {
				t.Errorf("got %s, wanted %s", got, tt.out)
			}
		})
	}
}

func TestTag(t *testing.T) {
	if s := Tag("Foo_Bar"); s != "ovsdb:\"Foo_Bar\"" {
		t.Fatalf("got %s, wanted ovsdb:\"Foo_Bar\"", s)
	}
}

func TestFileName(t *testing.T) {
	if s := FileName("foo"); s != "foo.go" {
		t.Fatalf("got %s, wanted foo.go", s)
	}
}

func TestCamelCase(t *testing.T) {
	cases := []struct {
		in       string
		expected string
	}{
		{"foo_bar_baz", "FooBarBaz"},
		{"foo-bar-baz", "FooBarBaz"},
		{"foos-bars-bazs", "FoosBarsBazs"},
		{"ip_port_mappings", "IPPortMappings"},
		{"external_ids", "ExternalIDs"},
		{"ip_prefix", "IPPrefix"},
		{"dns_records", "DNSRecords"},
		{"logical_ip", "LogicalIP"},
		{"ip", "IP"},
	}
	for _, tt := range cases {
		if s := camelCase(tt.in); s != tt.expected {
			t.Fatalf("got %s, wanted %s", s, tt.expected)
		}
	}
}

func ExampleNewTableTemplate() {
	schemaString := []byte(`
	{
		"name": "MyDB",
		"version": "0.0.0",
		"tables": {
			"table1": {
				"columns": {
					"string_column": {
						"type": "string"
					},
					"some_integer": {
						"type": "integer"
					}
				}
			}
		}
	}`)
	var schema ovsdb.DatabaseSchema
	err := json.Unmarshal(schemaString, &schema)
	if err != nil {
		panic(err)
	}

	base := NewTableTemplate()
	data := GetTableTemplateData("mypackage", "table1", schema.Table("table1"))

	// Add a function at after the struct definition
	// It can access the default data values plus any extra field that is added to data
	_, err = base.Parse(`{{define "postStructDefinitions"}}
func (t {{ index . "StructName" }}) {{ index . "FuncName"}}() string {
	return "bar"
}{{end}}`)
	if err != nil {
		panic(err)
	}
	data["FuncName"] = "TestFunc"

	gen, err := NewGenerator(WithDryRun())
	if err != nil {
		panic(err)
	}
	err = gen.Generate("generated.go", base, data)
	if err != nil {
		panic(err)
	}
}

func TestExtendedGenCloneableModel(t *testing.T) {
	a := &vswitchd.Bridge{}
	func(a interface{}) {
		_, ok := a.(model.CloneableModel)
		assert.True(t, ok, "is not cloneable")
	}(a)
}

func TestExtendedGenComparableModel(t *testing.T) {
	a := &vswitchd.Bridge{}
	func(a interface{}) {
		_, ok := a.(model.ComparableModel)
		assert.True(t, ok, "is not comparable")
	}(a)
}

func doGenDeepCopy(data model.CloneableModel, b *testing.B) {
	_ = data.CloneModel()
}

func doJSONDeepCopy(data model.CloneableModel, b *testing.B) {
	aBytes, err := json.Marshal(data)
	if err != nil {
		b.Fatal(err)
	}
	err = json.Unmarshal(aBytes, data)
	if err != nil {
		b.Fatal(err)
	}
}

func buildRandStr() *string {
	str := uuid.New().String()
	return &str
}

func buildTestBridge() *vswitchd.Bridge {
	return &vswitchd.Bridge{
		UUID:                *buildRandStr(),
		AutoAttach:          buildRandStr(),
		Controller:          []string{*buildRandStr(), *buildRandStr()},
		DatapathID:          buildRandStr(),
		DatapathType:        *buildRandStr(),
		DatapathVersion:     *buildRandStr(),
		ExternalIDs:         map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		FailMode:            &vswitchd.BridgeFailModeSecure,
		FloodVLANs:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		FlowTables:          map[int]string{1: *buildRandStr(), 2: *buildRandStr()},
		IPFIX:               buildRandStr(),
		McastSnoopingEnable: false,
		Mirrors:             []string{*buildRandStr(), *buildRandStr()},
		Name:                *buildRandStr(),
		Netflow:             buildRandStr(),
		OtherConfig:         map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		Ports:               []string{*buildRandStr(), *buildRandStr()},
		Protocols:           []string{*buildRandStr(), *buildRandStr()},
		RSTPEnable:          true,
		RSTPStatus:          map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		Sflow:               buildRandStr(),
		Status:              map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		STPEnable:           false,
	}
}

func buildTestInterface() *vswitchd.Interface {
	aBool := false
	aInt := 0
	return &vswitchd.Interface{
		UUID:                      *buildRandStr(),
		AdminState:                buildRandStr(),
		BFD:                       map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		BFDStatus:                 map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		CFMFault:                  &aBool,
		CFMFaultStatus:            []string{*buildRandStr(), *buildRandStr()},
		CFMFlapCount:              &aInt,
		CFMHealth:                 &aInt,
		CFMMpid:                   &aInt,
		CFMRemoteMpids:            []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		CFMRemoteOpstate:          buildRandStr(),
		Duplex:                    buildRandStr(),
		Error:                     buildRandStr(),
		ExternalIDs:               map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		Ifindex:                   &aInt,
		IngressPolicingBurst:      aInt,
		IngressPolicingKpktsBurst: aInt,
		IngressPolicingKpktsRate:  aInt,
		IngressPolicingRate:       aInt,
		LACPCurrent:               &aBool,
		LinkResets:                &aInt,
		LinkSpeed:                 &aInt,
		LinkState:                 buildRandStr(),
		LLDP:                      map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		MAC:                       buildRandStr(),
		MACInUse:                  buildRandStr(),
		MTU:                       &aInt,
		MTURequest:                &aInt,
		Name:                      *buildRandStr(),
		Ofport:                    &aInt,
		OfportRequest:             &aInt,
		Options:                   map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		OtherConfig:               map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		Statistics:                map[string]int{*buildRandStr(): 0, *buildRandStr(): 1},
		Status:                    map[string]string{*buildRandStr(): *buildRandStr(), *buildRandStr(): *buildRandStr()},
		Type:                      *buildRandStr(),
	}
}

func BenchmarkDeepCopy(b *testing.B) {
	bridge := buildTestBridge()
	intf := buildTestInterface()
	benchmarks := []struct {
		name       string
		data       model.CloneableModel
		deepCopier func(model.CloneableModel, *testing.B)
	}{
		{"modelgen Bridge", bridge, doGenDeepCopy},
		{"json Bridge", bridge, doJSONDeepCopy},
		{"modelgen Interface", intf, doGenDeepCopy},
		{"json Interface", intf, doJSONDeepCopy},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bm.deepCopier(bm.data, b)
			}
		})
	}
}

func doGenEquals(l, r model.ComparableModel, b *testing.B) {
	l.EqualsModel(r)
}

func doDeepEqual(l, r model.ComparableModel, b *testing.B) {
	reflect.DeepEqual(l, r)
}

func BenchmarkDeepEqual(b *testing.B) {
	bridge := buildTestBridge()
	intf := buildTestInterface()
	benchmarks := []struct {
		name       string
		left       model.ComparableModel
		right      model.ComparableModel
		comparator func(model.ComparableModel, model.ComparableModel, *testing.B)
	}{
		{"modelgen Bridge", bridge, bridge.DeepCopy(), doGenEquals},
		{"reflect Bridge", bridge, bridge.DeepCopy(), doDeepEqual},
		{"modelgen Interface", intf, intf.DeepCopy(), doGenEquals},
		{"reflect Interface", intf, intf.DeepCopy(), doDeepEqual},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bm.comparator(bm.left, bm.right, b)
			}
		})
	}
}
