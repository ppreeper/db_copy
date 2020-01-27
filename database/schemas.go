package database

import "fmt"

//////////
// Schemas
//////////

// Schema struct to hold schemas
type Schema struct {
	Name string `db:"SCHEMA_NAME"`
}

// GetSchemas returns schema list
func (db *Database) GetSchemas(d Dbase) ([]Schema, error) {
	q := ""
	if d.Driver == "postgres" {
		q = "select schema_name \"SCHEMA_NAME\" from sapdb.information_schema.schemata where schema_name not in ('pg_catalog','information_schema') order by schema_name"
	} else if d.Driver == "mssql" {
		q = "select \"SCHEMA_NAME\" from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME not in ("
		q += "'INFORMATION_SCHEMA',"
		q += "'db_accessadmin',"
		q += "'db_backupoperator',"
		q += "'db_datareader',"
		q += "'db_datawriter',"
		q += "'db_ddladmin',"
		q += "'db_denydatareader',"
		q += "'db_denydatawriter',"
		q += "'db_owner',"
		q += "'db_securityadmin',"
		q += "'sys'"
		q += ") order by SCHEMA_NAME"
	}
	ss := []Schema{}
	if err := db.Select(&ss, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return ss, nil
}
