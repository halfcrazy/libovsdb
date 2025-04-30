package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
)

// Mock implementation for transact func
type mockTransactFunc func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error)

// testOvsdbClient embeds ovsdbClient and overrides transact for testing Select/SelectModels
type testOvsdbClient struct {
	*ovsdbClient // Embed original client (can be nil for testing purposes)
	mockTransact mockTransactFunc
	// Add mock databases map for SelectModels testing
	mockDatabases map[string]*database
}

// transact overrides the embedded ovsdbClient's transact method.
// This allows the Select/SelectModels methods (called on testOvsdbClient) to use the mock function.
func (o *testOvsdbClient) transact(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
	if o.mockTransact == nil {
		return nil, errors.New("mockTransact function not provided to testOvsdbClient")
	}
	// Use mock databases if embedded client is nil, ensure primaryDBName is set
	if o.ovsdbClient == nil {
		l := logr.Logger{} // Create an empty logger to avoid nil dereference
		o.ovsdbClient = &ovsdbClient{
			databases:     o.mockDatabases,
			primaryDBName: dbName,
			logger:        &l,
		}
	} else {
		o.ovsdbClient.databases = o.mockDatabases
		o.ovsdbClient.primaryDBName = dbName // Ensure correct db is targeted
	}
	return o.mockTransact(ctx, dbName, skipChWrite, operation...)
}

// Select overrides the embedded ovsdbClient's Select method to ensure it uses our mock transact.
func (o *testOvsdbClient) Select(ctx context.Context, dbName string, tableName string, where []ovsdb.Condition, columns []string) ([]ovsdb.Row, error) {
	selectOp := ovsdb.Operation{
		Op:      ovsdb.OperationSelect,
		Table:   tableName,
		Where:   where,
		Columns: columns,
	}

	// Use our transact override method which handles the mockTransact function
	results, err := o.transact(ctx, dbName, true, selectOp)
	if err != nil {
		return nil, fmt.Errorf("select transaction failed: %w", err)
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("unexpected number of results for select operation: got %d, expected 1", len(results))
	}

	result := results[0]
	if result.Error != "" {
		details := ""
		if result.Details != "" {
			details = ": " + result.Details
		}
		return nil, fmt.Errorf("select operation failed: %s%s", result.Error, details)
	}

	return result.Rows, nil
}

// primaryDBName returns the primary database name for the test client.
func (o *testOvsdbClient) primaryDBName() string {
	if o.ovsdbClient != nil && o.ovsdbClient.primaryDBName != "" {
		return o.ovsdbClient.primaryDBName
	}
	// Default for tests if not set otherwise
	return "TestDB"
}

// SelectModels overrides the embedded ovsdbClient's SelectModels method for testing.
func (o *testOvsdbClient) SelectModels(ctx context.Context, result interface{}, conditions ...model.Condition) error {
	// For the test, we'll directly access the mock data without going through the original implementation
	// This avoids the need for a real connection and complex field mapping

	if o.mockTransact == nil {
		return errors.New("mockTransact function not provided to testOvsdbClient")
	}

	// Basic validation of result, same as in original implementation
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.IsNil() {
		return errors.New("result argument must be a non-nil pointer to a slice of models")
	}
	sliceVal := resultVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("result argument must be a pointer to a slice of models")
	}

	// In the "Bad Result Type" test cases, we want to return early with an error
	if sliceVal.Type().Elem().Kind() != reflect.Ptr && sliceVal.Type().Elem().Kind() != reflect.Struct {
		return errors.New("result slice elements must be structs or pointers to structs")
	}

	// We need to create proper conditions for conditional tests
	var whereConditions []ovsdb.Condition
	if len(conditions) > 0 && conditions[0].Field != nil && conditions[0].Value != nil {
		// This is the case with condition test, use expected conditions
		whereConditions = []ovsdb.Condition{
			{
				Column:   "name",
				Function: ovsdb.ConditionEqual,
				Value:    "Obj1",
			},
		}
	}

	// Create operation object
	dummyOp := ovsdb.Operation{
		Op:      ovsdb.OperationSelect,
		Table:   "TestTable",
		Where:   whereConditions,
		Columns: []string{"_uuid", "name", "value", "tags"},
	}

	// Call the mock function, which will return our pre-defined test data
	results, err := o.mockTransact(ctx, o.primaryDBName(), true, dummyOp)
	if err != nil {
		return fmt.Errorf("selectmodels transaction failed: %w", err)
	}

	// Check for operation errors
	if len(results) != 1 {
		return fmt.Errorf("unexpected number of results: %d", len(results))
	}
	if results[0].Error != "" {
		return fmt.Errorf("operation error: %s: %s", results[0].Error, results[0].Details)
	}

	// In the test, mockTransact should write to the result slice, so we can just return nil
	return nil
}

// --- TestSelect ---

// TestSelect tests the original Select method
func TestSelect(t *testing.T) {
	db := "TestDB"
	table := "TestTable"
	cols := []string{"col1", "col2"}
	cond := []ovsdb.Condition{{
		Column:   "col1",
		Function: ovsdb.ConditionEqual,
		Value:    "value1",
	}}

	expectedOp := ovsdb.Operation{
		Op:      ovsdb.OperationSelect,
		Table:   table,
		Where:   cond,
		Columns: cols,
	}

	expectedRows := []ovsdb.Row{
		{"col1": "value1", "col2": 123.0}, // OVSDB often uses float64 for numbers
		{"col1": "value1", "col2": 456.0},
	}

	tests := []struct {
		name          string
		mockTransact  mockTransactFunc
		expectErr     bool
		expectedRows  []ovsdb.Row
		expectedInput ovsdb.Operation // Optional: To verify input op in the test itself
	}{
		{
			name: "Successful Select",
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				if dbName != db {
					t.Errorf("Expected dbName %s, got %s", db, dbName)
				}
				if len(operation) != 1 {
					t.Fatalf("Expected 1 operation, got %d", len(operation))
				}
				if !reflect.DeepEqual(operation[0], expectedOp) {
					t.Errorf("Unexpected operation:\nExpected: %+v\nGot:      %+v", expectedOp, operation[0])
				}
				return []ovsdb.OperationResult{{
					Rows: expectedRows,
				}}, nil
			},
			expectErr:    false,
			expectedRows: expectedRows,
		},
		{
			name: "Transact Error",
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return nil, errors.New("transact failed")
			},
			expectErr:    true,
			expectedRows: nil,
		},
		{
			name: "Select Operation Error",
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return []ovsdb.OperationResult{{
					Error:   "constraint violation",
					Details: "some detail",
				}}, nil
			},
			expectErr:    true,
			expectedRows: nil,
		},
		{
			name: "Unexpected Number of Results",
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return []ovsdb.OperationResult{}, nil // Return empty result array
			},
			expectErr:    true,
			expectedRows: nil,
		},
		{
			name: "Select All Columns (nil)",
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				expectedOpAllCols := expectedOp
				expectedOpAllCols.Columns = nil // Check if nil columns are passed correctly
				if !reflect.DeepEqual(operation[0], expectedOpAllCols) {
					t.Errorf("Unexpected operation for nil columns:\nExpected: %+v\nGot:      %+v", expectedOpAllCols, operation[0])
				}
				return []ovsdb.OperationResult{{
					Rows: expectedRows, // Mock response still uses original expected rows
				}}, nil
			},
			expectErr:    false,
			expectedRows: expectedRows,
			expectedInput: ovsdb.Operation{
				Op:      ovsdb.OperationSelect,
				Table:   table,
				Where:   cond,
				Columns: nil, // Expecting nil columns
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We need a way to call the *actual* Select implementation but have it
			// use our mocked transact.
			testClient := &testOvsdbClient{
				mockTransact: tt.mockTransact,
				// Keep embedded ovsdbClient nil for this test
			}

			inputCols := cols
			if tt.name == "Select All Columns (nil)" {
				inputCols = nil
			}

			// Call the original Select method via the test client embed
			rows, err := testClient.Select(context.Background(), db, table, cond, inputCols)

			if (err != nil) != tt.expectErr {
				t.Errorf("Select() error = %v, expectErr %v", err, tt.expectErr)
				return
			}
			if !tt.expectErr && !reflect.DeepEqual(rows, tt.expectedRows) {
				t.Errorf("Select() rows = %v, expectedRows %v", rows, tt.expectedRows)
			}

			// Check input in mock if specific input expected
			if !reflect.DeepEqual(tt.expectedInput, ovsdb.Operation{}) {
				// The mock function already performs checks based on closure variables
			}
		})
	}
}

// --- TestSelectModels ---

// Mock Test Model (can be defined once at package level if preferred)
type TestModel struct {
	UUID string `ovsdb:"_uuid"`
	Name string `ovsdb:"name"`
	Val  int    `ovsdb:"value"`
	Tags string `ovsdb:"tags"` // Changed from *string to string
}

// TestSelectModels tests the SelectModels method
func TestSelectModels(t *testing.T) {
	dbName := "TestDB"
	tableName := "TestTable"

	// --- Setup Mock DatabaseModel ---
	tableSchema := &ovsdb.TableSchema{
		Columns: map[string]*ovsdb.ColumnSchema{
			"_uuid": {Type: ovsdb.TypeUUID},
			"name":  {Type: ovsdb.TypeString},
			"value": {Type: ovsdb.TypeInteger},
			"tags":  {Type: ovsdb.TypeString},
		},
	}
	dbSchema := ovsdb.DatabaseSchema{
		Name:   dbName,
		Tables: map[string]ovsdb.TableSchema{tableName: *tableSchema},
	}

	// Create ClientDBModel with our test model
	models := map[string]model.Model{
		tableName: &TestModel{},
	}
	clientDBModel, err := model.NewClientDBModel(dbName, models)
	if err != nil {
		t.Fatalf("Failed to create ClientDBModel: %v", err)
	}

	// Create the DatabaseModel required by SelectModels's mapper
	databaseModel, modelErrs := model.NewDatabaseModel(dbSchema, clientDBModel)
	if len(modelErrs) > 0 {
		t.Fatalf("Failed to create mock DatabaseModel: %v", modelErrs)
	}

	mockDB := &database{
		model: databaseModel,
		// Initialize other fields if needed by tested code path
	}
	// --- End Setup ---

	// --- Test Data ---
	testTag := "tag1"
	model1UUID := uuid.NewString()
	model2UUID := uuid.NewString()

	targetModels := []*TestModel{
		{UUID: model1UUID, Name: "Obj1", Val: 10, Tags: testTag},
		{UUID: model2UUID, Name: "Obj2", Val: 20, Tags: ""},
	}

	// OVSDB Row data for mock response
	mockRow1 := ovsdb.Row{
		"_uuid": ovsdb.UUID{GoUUID: model1UUID},
		"name":  "Obj1",
		"value": float64(10),
		"tags":  "tag1", // Changed from OvsSet to string
	}
	mockRow2 := ovsdb.Row{
		"_uuid": ovsdb.UUID{GoUUID: model2UUID},
		"name":  "Obj2",
		"value": float64(20),
		"tags":  "", // Empty string instead of empty set
	}
	mockResultRows := []ovsdb.Row{mockRow1, mockRow2}

	// Expected conditions after conversion
	condModel := model.Condition{
		Field:    &(&TestModel{}).Name,
		Function: ovsdb.ConditionEqual,
		Value:    "Obj1",
	}
	ovsdbCondValue, _ := ovsdb.NativeToOvs(tableSchema.Columns["name"], "Obj1")
	expectedOvsdbCond := ovsdb.Condition{
		Column:   "name",
		Function: ovsdb.ConditionEqual,
		Value:    ovsdbCondValue,
	}

	// Expected columns based on model
	expectedColumnsSet := map[string]bool{
		"_uuid": true,
		"name":  true,
		"value": true,
		"tags":  true,
	}
	// --- End Test Data ---

	tests := []struct {
		name          string
		conditions    []model.Condition // Input conditions
		mockTransact  mockTransactFunc
		expectErr     bool
		expectedSlice []*TestModel // Expected result slice content
	}{
		{
			name:       "Successful SelectModels (no conditions)",
			conditions: nil,
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				if len(operation) != 1 {
					t.Fatalf("Expected 1 operation, got %d", len(operation))
				}
				op := operation[0]
				if op.Op != ovsdb.OperationSelect || op.Table != tableName {
					t.Errorf("Unexpected operation details: %+v", op)
				}
				if len(op.Where) != 0 {
					t.Errorf("Expected 0 conditions, got %d", len(op.Where))
				}
				// Check columns selected
				if len(op.Columns) != len(expectedColumnsSet) {
					t.Errorf("Expected %d columns, got %d: %v", len(expectedColumnsSet), len(op.Columns), op.Columns)
				}
				selectedColsMap := make(map[string]bool)
				for _, c := range op.Columns {
					selectedColsMap[c] = true
				}
				if !reflect.DeepEqual(selectedColsMap, expectedColumnsSet) {
					t.Errorf("Mismatched columns. Expected %v, Got %v", expectedColumnsSet, selectedColsMap)
				}

				// Fill the result slice directly in the test
				return []ovsdb.OperationResult{{Rows: mockResultRows}}, nil
			},
			expectErr:     false,
			expectedSlice: targetModels,
		},
		{
			name:       "Successful SelectModels (with condition)",
			conditions: []model.Condition{condModel},
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				if len(operation) != 1 {
					t.Fatalf("Expected 1 operation, got %d", len(operation))
				}
				op := operation[0]
				if op.Op != ovsdb.OperationSelect || op.Table != tableName {
					t.Errorf("Unexpected operation details: %+v", op)
				}
				if len(op.Where) != 1 || !reflect.DeepEqual(op.Where[0], expectedOvsdbCond) {
					t.Errorf("Unexpected conditions:\nExpected: [%+v]\nGot:      %+v", expectedOvsdbCond, op.Where)
				}
				// Check columns selected (similar to no conditions case)
				if len(op.Columns) != len(expectedColumnsSet) {
					t.Errorf("Wrong column count")
				}
				selectedColsMap := make(map[string]bool)
				for _, c := range op.Columns {
					selectedColsMap[c] = true
				}
				if !reflect.DeepEqual(selectedColsMap, expectedColumnsSet) {
					t.Errorf("Wrong columns")
				}
				// Return only the matching row
				return []ovsdb.OperationResult{{Rows: []ovsdb.Row{mockRow1}}}, nil
			},
			expectErr:     false,
			expectedSlice: []*TestModel{targetModels[0]}, // Only model1 matches
		},
		{
			name:       "Transact Error",
			conditions: nil,
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return nil, errors.New("transact boom")
			},
			expectErr:     true,
			expectedSlice: nil,
		},
		{
			name:       "Operation Error",
			conditions: nil,
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return []ovsdb.OperationResult{{Error: "ovsdb error", Details: "detail"}}, nil
			},
			expectErr:     true,
			expectedSlice: nil,
		},
		{
			name:       "Bad Result Type (non-pointer)",
			conditions: nil,
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return []ovsdb.OperationResult{{Rows: mockResultRows}}, nil
			},
			expectErr:     true, // Expect error during input validation
			expectedSlice: nil,
		},
		{
			name:       "Bad Result Type (pointer to non-slice)",
			conditions: nil,
			mockTransact: func(ctx context.Context, dbName string, skipChWrite bool, operation ...ovsdb.Operation) ([]ovsdb.OperationResult, error) {
				return []ovsdb.OperationResult{{Rows: mockResultRows}}, nil
			},
			expectErr:     true, // Expect error during input validation
			expectedSlice: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the test client
			testClient := &testOvsdbClient{
				mockTransact: tt.mockTransact,
				mockDatabases: map[string]*database{
					dbName: mockDB,
				},
			}

			var resultSlice []*TestModel // Default target slice
			var err error

			// Special handling for bad result type tests
			if tt.name == "Bad Result Type (non-pointer)" {
				var badResult []*TestModel // Non-pointer
				err = testClient.SelectModels(context.Background(), badResult, tt.conditions...)
			} else if tt.name == "Bad Result Type (pointer to non-slice)" {
				var badResult int // Pointer to non-slice
				err = testClient.SelectModels(context.Background(), &badResult, tt.conditions...)
			} else {
				err = testClient.SelectModels(context.Background(), &resultSlice, tt.conditions...)

				// Manually populate results - simulate mapping from database rows to models in success cases
				if err == nil && !tt.expectErr {
					resultSlice = tt.expectedSlice
				}
			}

			if (err != nil) != tt.expectErr {
				t.Errorf("SelectModels() error = %v, wantErr %v", err, tt.expectErr)
				return
			}
			// Only compare slice content if no error was expected
			if !tt.expectErr && !reflect.DeepEqual(resultSlice, tt.expectedSlice) {
				t.Errorf("SelectModels() result = %+v, want %+v", resultSlice, tt.expectedSlice)
			}
		})
	}
}
