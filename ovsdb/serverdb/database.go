// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.

package serverdb

import "github.com/ovn-org/libovsdb/model"

type (
	DatabaseModel = string
)

var (
	DatabaseModelStandalone DatabaseModel = "standalone"
	DatabaseModelClustered  DatabaseModel = "clustered"
	DatabaseModelRelay      DatabaseModel = "relay"
)

// Database defines an object in Database table
type Database struct {
	UUID      string        `ovsdb:"_uuid"`
	Cid       *string       `ovsdb:"cid"`
	Connected bool          `ovsdb:"connected"`
	Index     *uint64       `ovsdb:"index"`
	Leader    bool          `ovsdb:"leader"`
	Model     DatabaseModel `ovsdb:"model"`
	Name      string        `ovsdb:"name"`
	Schema    *string       `ovsdb:"schema"`
	Sid       *string       `ovsdb:"sid"`
}

func copyDatabaseCid(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalDatabaseCid(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyDatabaseIndex(a *uint64) *uint64 {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalDatabaseIndex(a, b *uint64) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyDatabaseSchema(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalDatabaseSchema(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func copyDatabaseSid(a *string) *string {
	if a == nil {
		return nil
	}
	b := *a
	return &b
}

func equalDatabaseSid(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == b {
		return true
	}
	return *a == *b
}

func (a *Database) DeepCopyInto(b *Database) {
	*b = *a
	b.Cid = copyDatabaseCid(a.Cid)
	b.Index = copyDatabaseIndex(a.Index)
	b.Schema = copyDatabaseSchema(a.Schema)
	b.Sid = copyDatabaseSid(a.Sid)
}

func (a *Database) DeepCopy() *Database {
	b := new(Database)
	a.DeepCopyInto(b)
	return b
}

func (a *Database) CloneModelInto(b model.Model) {
	c := b.(*Database)
	a.DeepCopyInto(c)
}

func (a *Database) CloneModel() model.Model {
	return a.DeepCopy()
}

func (a *Database) Equals(b *Database) bool {
	return a.UUID == b.UUID &&
		equalDatabaseCid(a.Cid, b.Cid) &&
		a.Connected == b.Connected &&
		equalDatabaseIndex(a.Index, b.Index) &&
		a.Leader == b.Leader &&
		a.Model == b.Model &&
		a.Name == b.Name &&
		equalDatabaseSchema(a.Schema, b.Schema) &&
		equalDatabaseSid(a.Sid, b.Sid)
}

func (a *Database) EqualsModel(b model.Model) bool {
	c := b.(*Database)
	return a.Equals(c)
}

var _ model.CloneableModel = &Database{}
var _ model.ComparableModel = &Database{}
