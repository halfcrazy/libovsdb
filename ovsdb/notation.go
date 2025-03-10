package ovsdb

import "encoding/json"

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []interface{}            `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []Condition              `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid-name,omitempty"`
}

// MarshalJSON marshalls 'Operation' to a byte array
// For 'select' operations, we dont omit the 'Where' field
// to allow selecting all rows of a table
func (o Operation) MarshalJSON() ([]byte, error) {
	type OpAlias Operation
	switch o.Op {
	case "select":
		where := o.Where
		if where == nil {
			where = make([]Condition, 0)
		}
		return json.Marshal(&struct {
			Where []Condition `json:"where"`
			OpAlias
		}{
			Where:   where,
			OpAlias: (OpAlias)(o),
		})
	default:
		return json.Marshal(&struct {
			OpAlias
		}{
			OpAlias: (OpAlias)(o),
		})
	}
}

// MonitorRequests represents a group of monitor requests according to RFC7047
// We cannot use MonitorRequests by inlining the MonitorRequest Map structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type MonitorRequests struct {
	Requests map[string]MonitorRequest `json:"requests"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string       `json:"columns,omitempty"`
	Select  *MonitorSelect `json:"select,omitempty"`
}

// TableUpdates is a collection of TableUpdate entries
// We cannot use TableUpdates directly by json encoding by inlining the TableUpdate Map
// structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type TableUpdates struct {
	Updates map[string]TableUpdate `json:"updates"`
}

// TableUpdate represents a table update according to RFC7047
type TableUpdate struct {
	Rows map[string]RowUpdate `json:"rows"`
}

// RowUpdate represents a row update according to RFC7047
type RowUpdate struct {
	New Row `json:"new,omitempty"`
	Old Row `json:"old,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

// TransactResponse represents the response to a Transact Operation
type TransactResponse struct {
	Result []OperationResult `json:"result"`
	Error  string            `json:"error"`
}

// OperationResult is the result of an Operation
type OperationResult struct {
	Count   int         `json:"count,omitempty"`
	Error   string      `json:"error,omitempty"`
	Details string      `json:"details,omitempty"`
	UUID    UUID        `json:"uuid,omitempty"`
	Rows    []ResultRow `json:"rows,omitempty"`
}

func ovsSliceToGoNotation(val interface{}) (interface{}, error) {
	switch sl := val.(type) {
	case []interface{}:
		bsliced, err := json.Marshal(sl)
		if err != nil {
			return nil, err
		}

		switch sl[0] {
		case "uuid", "named-uuid":
			var uuid UUID
			err = json.Unmarshal(bsliced, &uuid)
			return uuid, err
		case "set":
			var oSet OvsSet
			err = json.Unmarshal(bsliced, &oSet)
			return oSet, err
		case "map":
			var oMap OvsMap
			err = json.Unmarshal(bsliced, &oMap)
			return oMap, err
		}
		return val, nil
	}
	return val, nil
}

type Mutator string

const (
	MutateOperationDelete    Mutator = "delete"
	MutateOperationInsert    Mutator = "insert"
	MutateOperationAdd       Mutator = "+="
	MutateOperationSubstract Mutator = "-="
	MutateOperationMultiply  Mutator = "*="
	MutateOperationDivide    Mutator = "/="
	MutateOperationModulo    Mutator = "%="
)
